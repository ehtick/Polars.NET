use std::{ffi::c_char, time::{SystemTime, UNIX_EPOCH}};
use deltalake::{DeltaTable, Path, kernel::{Action, Remove, transaction}, protocol::DeltaOperation};
use futures::StreamExt;
use serde_json::Value;
use uuid::Uuid;
use polars::{error::{PolarsError, PolarsResult}, prelude::*};
use deltalake::protocol::SaveMode;

use crate::pl_io::{io_utils::build_unified_sink_args, parquet::parquet_utils::build_parquet_write_options};
use crate::types::{LazyFrameContext,SelectorContext};
use crate::utils::ptr_to_str;
use crate::delta::utils::*;
use crate::delta::merge::phase_process_staging;

#[unsafe(no_mangle)]
pub extern "C" fn pl_sink_delta(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    mode: u8,
    can_evolve: bool,
    // --- Partition Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,

    // --- Parquet Options ---
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,
    compat_level: i32,
    
    // --- Unified Options ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    
    // --- Cloud Params ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64,  
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) {
ffi_try_void!({
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let base_path_str = ptr_to_str(base_path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 1. Get Schema & Parse Partition Column
        let schema = lf_ctx.inner.collect_schema()
            .map_err(|e| PolarsError::ComputeError(format!("Failed to collect schema: {}", e).into()))?;

        let partition_cols = if !partition_by_ptr.is_null() {
            let selector_ctx = unsafe { &*partition_by_ptr };
            let ignored = PlHashSet::new();
            selector_ctx.inner.into_columns(&schema, &ignored)?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
        } else {
            Vec::new()
        };
        let rt = get_runtime();
        let save_mode = map_savemode(mode);
        let write_id = Uuid::new_v4(); 

        // 2. Build Staging Path
        // Example: s3://bucket/table/.tmp_write_<uuid>
        let staging_dir_name = format!(".tmp_write_{}", write_id);
        
        let staging_uri = if base_path_str.contains("://") {
            // Remote Path
            format!("{}/{}", base_path_str.trim_end_matches('/'), staging_dir_name)
        } else {
            // Local Path
            std::path::Path::new(base_path_str)
                .join(&staging_dir_name)
                .to_string_lossy()
                .to_string()
        };
        // 3. Build Cloud Options & Delta Table
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        let (mut table, should_skip) = rt.block_on(async {
            let table_url = parse_table_url(base_path_str)?;
            let dt = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta init error: {}", e).into()))?;
            
            // let mut old_files = Vec::new();
            let mut skip_write = false;

            if dt.version() >= Some(0) {
                match save_mode {
                    // Case A: ErrorIfExists -> Throw Exception
                    SaveMode::ErrorIfExists => {
                        return Err(PolarsError::ComputeError(format!("Table already exists at {}", table_url).into()));
                    },
                    // Case B: Ignore -> Set as skip
                    SaveMode::Ignore => {
                        skip_write = true;
                    },
                    // },
                    // Case C: Append -> Continue
                    _ => {} 
                }
            }
            Ok::<_, PolarsError>((dt,  skip_write))
        })?;

        if should_skip {
            return Ok(());
        }

        // // 4. Polars Sink -> Staging Folder

        // =========================================================
        // Align Partitioning Logic BEFORE building destination
        // =========================================================
        let mut final_partition_cols = partition_cols.clone();
        
        if table.version() >= Some(0) {
            let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot: {}", e).into()))?;
            let existing_part_cols = snapshot.metadata().partition_columns().clone();

            if !existing_part_cols.is_empty() {
                if partition_cols.is_empty() {
                    // Case A: Auto-fill from table definition
                    for col in &existing_part_cols {
                        if schema.get_field(col).is_none() {
                             return Err(PolarsError::ComputeError(format!("DataFrame missing partition column: {}", col).into()));
                        }
                    }
                    final_partition_cols = existing_part_cols;
                } else if partition_cols != existing_part_cols {
                    // Case B: Mismatch
                    return Err(PolarsError::ComputeError(
                         format!("Partition mismatch. Table: {:?}, Input: {:?}", existing_part_cols, partition_cols).into()
                     ));
                }
            }
        }
        
        // 4. Build Destination (Use final_partition_cols)
        
        let partition_strategy = if final_partition_cols.is_empty() {
             PartitionStrategy::FileSize
        } else {
             // Convert Column name to Expr
             let keys: Vec<Expr> = final_partition_cols.iter().map(|n| col(n)).collect();
             PartitionStrategy::Keyed {
                 keys,
                 include_keys: include_keys, 
                 keys_pre_grouped: keys_pre_grouped,
             }
        };

        let hive_provider = file_provider::HivePathProvider {
            extension: PlSmallStr::from_str(".parquet"),
        };
        
        let destination = SinkDestination::Partitioned {
            base_path: PlRefPath::new(&staging_uri), 
            file_path_provider: Some(file_provider::FileProviderType::Hive(hive_provider)),
            partition_strategy,
            max_rows_per_file: if max_rows_per_file == 0 { u32::MAX } else { max_rows_per_file as u32 },
            approximate_bytes_per_file: if approx_bytes_per_file == 0 { usize::MAX as u64 } else { approx_bytes_per_file },
        };

        let write_options_arc = build_parquet_write_options(
            compression, compression_level, statistics, row_group_size, data_page_size, compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);
        
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir, maintain_order, sync_on_close,
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                cloud_keys, cloud_values, cloud_len
            )
        };

        lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect_with_engine(Engine::Streaming)?;
            
        // 5. Staging -> Move -> Commit
        rt.block_on(async {
            // A. Auto Create Table
            if table.version() < Some(0) {
                 let delta_schema = convert_to_delta_schema(&schema)?;
                 table = table.create()
                    .with_columns(delta_schema.fields().cloned())
                    .with_partition_columns(partition_cols.clone()) 
                    .await
                    .map_err(|e| PolarsError::ComputeError(format!("Create table error: {}", e).into()))?;
            }

            let object_store = table.object_store();

            let mut actions = Vec::new();
            // ==========================================
            // Schema Evolution Check
            // ==========================================
            // 1. Get Newest Schema
            let current_snapshot = table.snapshot()
                .map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
            let current_delta_schema = current_snapshot.schema();
            
            // Convert Polars Schema to Delta Schema
            let new_delta_schema = convert_to_delta_schema(&schema)?;

            // Compare Schema 
            if current_delta_schema.as_ref() != &new_delta_schema {
                if !can_evolve {
                    return Err(PolarsError::ComputeError(
                        "Schema mismatch detected. If you want to evolve the schema, enable 'can_evolve'.".into()
                    ));
                }

                // Build Metadata Action 
                let current_metadata = current_snapshot.metadata();

                // Serlize JSON Value
                let mut meta_json = serde_json::to_value(current_metadata)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize metadata: {}", e).into()))?;

                // Generate New Schema JSON String
                let new_schema_string = serde_json::to_string(&new_delta_schema)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize new schema: {}", e).into()))?;

                // Modify JSON "schemaString" field
                if let Some(obj) = meta_json.as_object_mut() {
                    obj.insert("schemaString".to_string(), Value::String(new_schema_string));
                } else {
                    return Err(PolarsError::ComputeError("Metadata is not a JSON object".into()));
                }

                // Deserilize to Metadata struct
                let new_metadata_action: deltalake::kernel::Metadata = serde_json::from_value(meta_json)
                    .map_err(|e| PolarsError::ComputeError(format!("Failed to recreate metadata: {}", e).into()))?;

                // Inject Metadata Action
                actions.insert(0, Action::Metadata(new_metadata_action));
            }
            // ==========================================
            // Step B: Parallel Staging Processing
            // ==========================================
            // Fast Listing
            let add_actions = phase_process_staging(&table, &staging_dir_name, &final_partition_cols, write_id).await?;
            
            for res in add_actions {
                actions.push(res);
            }

            // Handle Overwrite
            if let SaveMode::Overwrite = save_mode {
                let mut stream = table.get_active_add_actions_by_partitions(&[]);
                
                while let Some(view_res) = stream.next().await {
                    let view = view_res.map_err(|e| PolarsError::ComputeError(format!("List files error: {}", e).into()))?;
                    
                    let add_action = view_to_add_action(&view);
                    
                    let remove = Remove {
                        path: add_action.path.clone(),
                        deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                        data_change: true,
                        extended_file_metadata: Some(true),
                        partition_values: Some(add_action.partition_values),
                        size: Some(add_action.size),
                        deletion_vector: add_action.deletion_vector,
                        tags: add_action.tags,
                        base_row_id: add_action.base_row_id,
                        default_row_commit_version: add_action.default_row_commit_version,
                    };
                    actions.push(Action::Remove(remove));
                }
            }

            if actions.is_empty() {
                return Ok::<(), PolarsError>(());
            }

            // Commit
            let operation = DeltaOperation::Write {
                mode: save_mode,
                partition_by: if !partition_cols.is_empty() { Some(partition_cols) } else { None },
                predicate: None,
            };

            let _ver = transaction::CommitBuilder::default()
                .with_actions(actions)
                .build(
                    table.state.as_ref().map(|s| s as &dyn transaction::TableReference), 
                    table.log_store().clone(), 
                    operation
                )
                .await
                .map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
            let _ = object_store.delete(&Path::from(staging_dir_name)).await;
            Ok(())
        })?;
        
        Ok(())
    })
}
