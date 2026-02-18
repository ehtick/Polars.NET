use std::{ffi::c_char, time::{SystemTime, UNIX_EPOCH}};
use deltalake::{DeltaTable, Path, kernel::{Action, Remove, transaction}, protocol::DeltaOperation};
// use polars_utils::IdxSize;
use serde_json::Value;
use uuid::Uuid;
use polars::{error::{PolarsError, PolarsResult}, prelude::*};
use deltalake::protocol::SaveMode;

use crate::io::{build_parquet_write_options, build_partitioned_destination, build_unified_sink_args};
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
        
        let (mut table, old_files_to_remove, should_skip) = rt.block_on(async {
            let table_url = parse_table_url(base_path_str)?;
            let dt = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta init error: {}", e).into()))?;
            
            let mut old_files = Vec::new();
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
                    // Case C: Overwrite -> Collect old files
                    SaveMode::Overwrite => {
                        let files = dt.get_files_by_partitions(&[]).await
                            .map_err(|e| PolarsError::ComputeError(format!("List files error: {}", e).into()))?;
                        for p in files {
                            old_files.push(p.to_string());
                        }
                    },
                    // Case D: Append -> Continue
                    _ => {} 
                }
            }
            Ok::<_, PolarsError>((dt, old_files, skip_write))
        })?;

        if should_skip {
            return Ok(());
        }

        // 4. Polars Sink -> Staging Folder
        let destination = unsafe {
            build_partitioned_destination(
                std::ffi::CString::new(staging_uri.clone()).unwrap().as_ptr(), 
                ".parquet", 
                &schema,
                partition_by_ptr,
                include_keys,
                keys_pre_grouped,
                max_rows_per_file,
                approx_bytes_per_file
            )?
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
            let add_actions = phase_process_staging(&table, &staging_dir_name, &partition_cols, write_id).await?;
            
            for res in add_actions {
                actions.push(res);
            }

            // Handle Overwrite
            if let SaveMode::Overwrite = save_mode {
                for old_path in old_files_to_remove {
                     let remove = Remove {
                         path: old_path,
                         deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                         data_change: true,
                         ..Default::default()
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

// pub(crate) async fn write_lazyframe_to_temp_files(
//     table: &DeltaTable,
//     lf: polars::prelude::LazyFrame,
//     partition_cols: Vec<String>,
//     write_id: Uuid,
//     cloud_options: Option<polars_io::cloud::CloudOptions>,
// ) -> Result<Vec<Action>, PolarsError> {
//     // 1. 路径处理 (Url -> OS Path / Cloud Path)
//     let table_url = table.table_url();
//     let staging_dir_name = format!(".tmp_write_{}", write_id);

//     // 2026年 Polars 需要明确的基础路径类型
//     let staging_uri = if table_url.scheme() == "file" {
//         table_url.to_file_path()
//             .map_err(|_| PolarsError::ComputeError("Failed to convert file URL".into()))?
//             .join(&staging_dir_name)
//             .to_string_lossy()
//             .to_string()
//     } else {
//         let url_str = table_url.as_str().trim_end_matches('/').to_string();
//         format!("{}/{}", url_str, staging_dir_name)
//     };

//     // 2. 准备 Parquet Write Options
//     let write_options = ParquetWriteOptions {
//         compression: Default::default(),
//         statistics: Default::default(),
//         row_group_size: None,
//         data_page_size: None,
//         key_value_metadata: None,
//         arrow_schema: None,
//         compat_level: None,
//     };
//     let file_format = FileWriteFormat::Parquet(std::sync::Arc::new(write_options));

//     // 3. 构建 UnifiedSinkArgs
//     let unified_args = UnifiedSinkArgs {
//         mkdir: true,
//         maintain_order: false,
//         sync_on_close: polars::prelude::sync_on_close::SyncOnCloseType::None,
//         cloud_options: cloud_options.map(std::sync::Arc::new),
//     };

//     // ---------------------------------------------------------
//     // 4. 构建复杂的 SinkDestination (从 FFI 逻辑移植)
//     // ---------------------------------------------------------
    
//     // 4.1 转换分区列： Vec<String> -> Vec<Expr>
//     let partition_exprs: Vec<Expr> = partition_cols.iter()
//         .map(|name| polars::prelude::col(name)) // 2026 Polars col() 接受 Into<PlSmallStr>
//         .collect();

//     // 4.2 确定 PartitionStrategy
//     // 既然是 Delta Lake 的 optimize/write，如果有分区列，我们通常使用 Keyed 策略 (Hive Style)
//     // 如果没有分区列，则回退到 FileSize 策略 (依赖 max_rows 或 bytes 切分)
//     let partition_strategy = if partition_exprs.is_empty() {
//         PartitionStrategy::FileSize
//     } else {
//         PartitionStrategy::Keyed {
//             keys: partition_exprs,
//             include_keys: false, // Delta 通常在 Parquet 文件内部不存储分区列数据以节省空间 (由目录结构定义)
//             keys_pre_grouped: false,
//         }
//     };

//     // 4.3 构建 HivePathProvider
//     // 指定扩展名为 .parquet
//     let hive_provider = file_provider::HivePathProvider {
//         extension: PlSmallStr::from_static(".parquet"), 
//     };
//     let file_path_provider = Some(polars::prelude::file_provider::FileProviderType::Hive(hive_provider));

//     // 4.4 组装最终的 Destination
//     // 注意：这里需要设定文件切分的阈值。
//     // 由于函数签名没有传入这些参数，我使用了 2026 年常见的默认“大文件”设置。
//     // 如果需要细粒度控制，建议将 max_rows_per_file 等添加到函数参数中。
//     let destination = SinkDestination::Partitioned {
//         base_path: PlRefPath::new(&staging_uri), // 使用 PlRefPath 包装路径
//         file_path_provider,
//         partition_strategy,
//         // 下面两个参数决定了非 Keyed 模式下的切分逻辑，或者 Keyed 模式下每个分区内的文件切分
//         max_rows_per_file: usize::MAX as IdxSize, // 默认不限制行数，尽量写大文件
//         approximate_bytes_per_file: u64::MAX,     // 默认不限制大小
//     };

//     // ---------------------------------------------------------

//     // 5. 执行 Sink
//     lf.sink(destination, file_format, unified_args)?
//         .collect_with_engine(Engine::Streaming)?;

//     // 6. 生成 Actions
//     let add_actions = phase_process_staging(table, &staging_dir_name, &partition_cols, write_id).await?;

//     Ok(add_actions)
// }