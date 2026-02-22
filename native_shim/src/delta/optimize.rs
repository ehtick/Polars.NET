use std::{collections::HashMap, ffi::c_char, time::{SystemTime, UNIX_EPOCH}};
use futures::StreamExt;
use polars::prelude::*;
use polars_buffer::Buffer;
use url::Url;
use deltalake::{DeltaTable, PartitionFilter, PartitionValue, Path, kernel::scalars::ScalarExt};
use deltalake::kernel::{Action, Add, Remove, transaction::CommitBuilder};
use deltalake::protocol::DeltaOperation;
use uuid::Uuid;

use crate::{delta::{deletion_vector::{apply_deletion_vector, read_deletion_vector}, merge::phase_process_staging, utils::*}, pl_io::parquet::parquet_utils::build_parquet_write_options, utils::ptr_to_vec_string};
use crate::pl_io::io_utils::{build_cloud_options, build_unified_sink_args};
use crate::utils::ptr_to_str;

// =========================================================
// 0. Context Definition
// =========================================================

struct OptimizeContext {
    pub table_url: Url,
    pub target_size_bytes: i64,
    pub partition_filters: Option<HashMap<String, String>>,// Optional filter
    pub z_order_columns: Option<Vec<String>>
}

/// A bin means a separated optimize mission
#[derive(Debug, Clone)]
struct OptimizeBin {
    pub partition_values: HashMap<String, Option<String>>, 
    pub files: Vec<Add>,   
    pub total_size: i64,   
}

// =========================================================
// Phase 1: Analysis (Bin-packing Strategy)
// =========================================================

/// Scan table to plan optimization bins
async fn phase_1_plan_bins(
    ctx: &OptimizeContext,
    table: &DeltaTable,
) -> PolarsResult<Vec<OptimizeBin>> {
    
    // =========================================================
    // Build PartitionFilters
    // =========================================================
    let mut filters = Vec::new();
    if let Some(pf_map) = &ctx.partition_filters {
        for (key, value) in pf_map {
            filters.push(PartitionFilter {
                key: key.clone(),
                value: PartitionValue::Equal(value.clone()), 
            });
        }
    }
    
    // 1. Get active add actions
    let mut stream = table.get_active_add_actions_by_partitions(&filters);

    // Key = Canonical Partition String
    let mut buckets: HashMap<String, Vec<Add>> = HashMap::new();

    let min_rewrite_threshold = ctx.target_size_bytes / 2;

    while let Some(view_res) = stream.next().await {
        let view = view_res.map_err(|e| PolarsError::ComputeError(format!("Delta stream error: {}", e).into()))?;
        
        // =========================================================
        // Build Partition values map
        // =========================================================
        let mut partition_values_map = HashMap::new();
        
        if let Some(struct_data) = view.partition_values() {
            let fields = struct_data.fields();
            let values = struct_data.values();
            
            for (field, val) in fields.iter().zip(values.iter()) {
                let name = field.name().to_string();
                let value = if val.is_null() {
                    None
                } else {
                    Some(val.serialize())
                };
                partition_values_map.insert(name, value);
            }
        }

        // =========================================================
        // Conversion to Add Action
        // =========================================================

        let dv_descriptor = view.deletion_vector_descriptor();

        let add = Add {
            path: view.path().to_string(),
            size: view.size(),
            partition_values: partition_values_map,
            modification_time: view.modification_time(),
            data_change: false, 
            stats: view.stats(), 
            tags: None,
            deletion_vector: dv_descriptor.clone(), 
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        // =========================================================
        // Small Files OR Dirty Files
        // =========================================================
        let is_small_file = add.size < min_rewrite_threshold;
        let has_dv = dv_descriptor.is_some();

        if !is_small_file && !has_dv {
            continue;
        }

        // Generate partition Key
        let part_key = if add.partition_values.is_empty() {
            "__unpartitioned__".to_string()
        } else {
            let mut keys: Vec<&String> = add.partition_values.keys().collect();
            keys.sort();
            keys.iter().map(|k| {
                format!("{}={}", k, add.partition_values.get(*k).unwrap_or(&Some("null".into())).as_deref().unwrap_or("null"))
            }).collect::<Vec<_>>().join("/")
        };

        buckets.entry(part_key).or_default().push(add);
    }

    // Bin-packing (Greedy)
    let mut final_tasks = Vec::new();

    let max_bin_size = (ctx.target_size_bytes as f64 * 1.2) as i64;
    
    for (_part_key, mut files) in buckets {
        if files.is_empty() { continue; }

        files.sort_by_key(|f| f.size);
        
        // Get Partition Values
        let partition_values = files[0].partition_values.clone();

        let mut current_bin_files = Vec::new();
        let mut current_bin_size = 0;

        for file in files {
            // Greedy：if current bin size + current file size > target size -> close bin
            if current_bin_size > 0 && (current_bin_size + file.size) > max_bin_size {
                
                // Write Amplification Check
                // Only files >1 in bin
                if current_bin_files.len() > 1 {
                    final_tasks.push(OptimizeBin {
                        partition_values: partition_values.clone(), 
                        files: std::mem::take(&mut current_bin_files), 
                        total_size: current_bin_size,
                    });
                } else {
                    current_bin_files.clear();
                }
                
                current_bin_size = 0;
            }

            current_bin_size += file.size;
            current_bin_files.push(file);
        }

        // Handle Residual Bin
        if !current_bin_files.is_empty() {
            if current_bin_files.len() > 1 {
                final_tasks.push(OptimizeBin {
                    partition_values: partition_values, 
                    files: current_bin_files,
                    total_size: current_bin_size,
                });
            }
        }
    }

    Ok(final_tasks)
}

// =========================================================
// Phase 2: Execution (Polars Read -> Write Staging)
// =========================================================

fn phase_2_execute_rewrite(
    ctx: &OptimizeContext,
    table: &DeltaTable, 
    bin: &OptimizeBin,
    cloud_args: &RawCloudArgs,
) -> PolarsResult<(String, Uuid)> {
    
    // 1. Setup Identity
    let write_id = Uuid::new_v4();
    let staging_dir_name = format!(".optimize_staging_{}", write_id);
    let root_trimmed = ctx.table_url.as_str().trim_end_matches('/');
    let staging_uri = format!("{}/{}", root_trimmed, staging_dir_name);
    
    println!(
        "Optimizing bin: {} files, total size: {:.3} MB", 
        bin.files.len(),
        bin.total_size as f64 / 1024.0 / 1024.0
    );

    // =========================================================
    // 2. Construct Reader (Handling Deletion Vectors)
    // =========================================================
    let has_dv = bin.files.iter().any(|f| f.deletion_vector.is_some());

    // Build ScanArgs
    let make_scan_args = || unsafe {
        let mut args = ScanArgsParquet::default();
        args.hive_options = HiveOptions {
            enabled: Some(true), 
            hive_start_idx: 0, 
            schema: None, 
            try_parse_dates: true,
        };
        args.cloud_options = build_cloud_options(
            cloud_args.provider, cloud_args.retries, cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms, cloud_args.retry_max_backoff_ms, cloud_args.cache_ttl,
            cloud_args.keys, cloud_args.values, cloud_args.len,
        );
        args.low_memory = true;
        args.rechunk = false;
        
        // If has DV, turn on row index
        if has_dv {
            args.row_index = Some(RowIndex { 
                name: "__row_index".into(), 
                offset: 0 
            });
            args.include_file_paths = Some(PlSmallStr::from_static("__file_path"));
        }
        args
    };

    let mut lf = if !has_dv {
        // ---------------------------------------------------------
        // Scenario A: Fast Path (No DVs in this bin)
        // ---------------------------------------------------------
        let full_paths: Vec<PlRefPath> = bin.files.iter()
            .map(|f| PlRefPath::new(format!("{}/{}", root_trimmed, f.path)))
            .collect();
        LazyFrame::scan_parquet_files(Buffer::from(full_paths), make_scan_args())?
    } else {
        // ---------------------------------------------------------
        // Scenario B: Slow Path (DVs present, need filtering)
        // ---------------------------------------------------------
        let mut lfs = Vec::new();
        let mut clean_paths = Vec::new();
        let mut dirty_files = Vec::new();

        for f in &bin.files {
            if f.deletion_vector.is_some() {
                dirty_files.push(f);
            } else {
                clean_paths.push(format!("{}/{}", root_trimmed, f.path));
            }
        }

        // Read Clean files
        if !clean_paths.is_empty() {
            let pl_paths: Vec<PlRefPath> = clean_paths.iter().map(|s| PlRefPath::new(s)).collect();
            lfs.push(LazyFrame::scan_parquet_files(Buffer::from(pl_paths), make_scan_args())?);
        }

        // Read Dirty files then filter with DV
        if !dirty_files.is_empty() {
            let rt = get_runtime();
            let object_store = table.object_store();
            let table_root = Path::from(root_trimmed);

            let dirty_lfs = rt.block_on(async {
                let mut processed = Vec::with_capacity(dirty_files.len());
                for f in dirty_files {
                    let full_path = format!("{}/{}", root_trimmed, f.path);
                    
                    let mut single_lf = LazyFrame::scan_parquet(
                        PlRefPath::new(&full_path), 
                        make_scan_args()
                    )?;

                    // Apply DV
                    if let Some(dv) = &f.deletion_vector {
                        let bitmap = read_deletion_vector(object_store.clone(), dv, &table_root).await?;
                        single_lf = apply_deletion_vector(single_lf, bitmap)?;
                    }
                    processed.push(single_lf);
                }
                Ok::<_, PolarsError>(processed)
            })?;
            lfs.extend(dirty_lfs);
        }

        let args = UnionArgs {
            parallel: true, rechunk: false, to_supertypes: true, diagonal: true, ..Default::default()
        };
        polars::prelude::concat(lfs, args)?
    };

    // Drop row index column
    if has_dv {
        lf = lf.drop(Selector::ByName { 
            names: Arc::from(vec![PlSmallStr::from_static("__row_index"),
                PlSmallStr::from_static("__file_path")]),
            strict: false 
        });
    }

    // =========================================================
    // 3. Transformations (Z-Order & Partitions)
    // =========================================================
    if let Some(z_cols) = &ctx.z_order_columns {
        lf = crate::delta::zorder::apply_z_order(lf, z_cols)?;
    }

    if !bin.partition_values.is_empty() {
        let part_cols: Vec<PlSmallStr> = bin.partition_values.keys()
            .map(|k| PlSmallStr::from_str(k)) 
            .collect();
        
        lf = lf.drop(Selector::ByName { 
            names: Arc::from(part_cols), 
            strict: false 
        });
    }

    // =========================================================
    // 4. Sink to Staging
    // =========================================================
    let mut part_path = String::new();
    if !bin.partition_values.is_empty() {
        let mut entries: Vec<(&String, &Option<String>)> = bin.partition_values.iter().collect();
        entries.sort_by_key(|e| e.0);
        let paths: Vec<String> = entries.iter().map(|(k, v)| {
            format!("{}={}", k, v.as_deref().unwrap_or("__HIVE_DEFAULT_PARTITION__"))
        }).collect();
        part_path = paths.join("/");
    }

    let file_name = format!("part-{}-optimized.parquet", Uuid::new_v4());
    let dest_path_str = if part_path.is_empty() {
        format!("{}/{}", staging_uri, file_name)
    } else {
        format!("{}/{}/{}", staging_uri, part_path, file_name)
    };
    
    let write_opts = build_parquet_write_options(1, -1, true, 0, 0, -1)
        .map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;

    let unified_args = unsafe { build_unified_sink_args(
        true, false, 0, 
        cloud_args.provider, cloud_args.retries, cloud_args.retry_timeout_ms,
        cloud_args.retry_init_backoff_ms, cloud_args.retry_max_backoff_ms, cloud_args.cache_ttl,
        cloud_args.keys, cloud_args.values, cloud_args.len,
    )};

    let destination = SinkDestination::File {
        target: SinkTarget::Path(PlRefPath::from(dest_path_str.as_str())),
    };

    lf.sink(destination, FileWriteFormat::Parquet(write_opts), unified_args)?
        .collect_with_engine(Engine::Streaming)?;

    Ok((staging_dir_name, write_id))
}

// =========================================================
// Phase 3: Commit (Transaction)
// =========================================================

async fn phase_3_commit_optimize(
    table: &mut DeltaTable,
    bin: &OptimizeBin,
    staging_dir: &str,
    write_id: Uuid,
) -> PolarsResult<()> {
    
    // 1. Promote Staging Files
    let partition_cols: Vec<String> = bin.partition_values.keys().cloned().collect();
    
    let new_add_actions = phase_process_staging(
        table, 
        staging_dir, 
        &partition_cols, 
        write_id
    ).await?;

    // If no new data, clear and return
    if new_add_actions.is_empty() {
        let object_store = table.object_store();
        let _ = object_store.delete(&Path::from(staging_dir)).await;
        return Ok(());
    }

    // 2. Construct Remove Actions (Source files)
    let remove_actions: Vec<Action> = bin.files.iter().map(|f| {
        Action::Remove(Remove {
            path: f.path.clone(),
            deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
            data_change: false, 
            extended_file_metadata: Some(true),
            partition_values: Some(f.partition_values.clone()),
            size: Some(f.size),
            deletion_vector: f.deletion_vector.clone(),
            ..Default::default()
        })
    }).collect();

    // 3. Combine Actions
    let mut actions = Vec::with_capacity(new_add_actions.len() + remove_actions.len());
    actions.extend(remove_actions);
    actions.extend(new_add_actions);

    // 4. Commit
    let operation = DeltaOperation::Optimize {
        target_size: 0, 
        predicate: None,
    };

    let commit_res = CommitBuilder::default()
        .with_actions(actions)
        .with_max_retries(0)
        .build(
            Some(table.snapshot().map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?), 
            table.log_store().clone(),
            operation
        )
        .await;

    // 5. Cleanup Staging
    let object_store = table.object_store();
    let _ = object_store.delete(&Path::from(staging_dir)).await;

    commit_res.map_err(|e| PolarsError::ComputeError(format!("Optimize Commit failed: {}", e).into()))?;

    Ok(())
}

// =========================================================
// Main Entry (Retry Loop & FFI)
// =========================================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_optimize(
    target_path_ptr: *const c_char,
    target_size_mb: i64,
    filter_json_ptr: *const c_char,
    // Z-Order Params
    z_order_cols_ptr: *const *const c_char,
    z_order_len: usize,
    // --- Cloud Args ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64, 
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize,

    // --- Output ---
    out_num_files_optimized: *mut usize,
) {
    ffi_try_void!({
        // 1. Setup Context
        let path_str = ptr_to_str(target_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        let partition_filters = if !filter_json_ptr.is_null() {
            let json_str = ptr_to_str(filter_json_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            if json_str.trim().is_empty() {
                None
            } else {
                // Parse to Key-Value Map
                // Example: {"date": "2024-01-01", "region": "us"}
                let map: HashMap<String, String> = serde_json::from_str(json_str)
                    .map_err(|e| PolarsError::ComputeError(format!("Invalid filter JSON: {}", e).into()))?;
                Some(map)
            }
        } else {
            None
        };
        let z_order_columns = if z_order_len > 0 && !z_order_cols_ptr.is_null() {
            unsafe {Some(ptr_to_vec_string(z_order_cols_ptr, z_order_len))}
        } else {
            None
        };

        let ctx = OptimizeContext {
            table_url: table_url.clone(),
            target_size_bytes: target_size_mb * 1024 * 1024,
            partition_filters:partition_filters, 
            z_order_columns,
        };


        let cloud_args = RawCloudArgs {
            provider: cloud_provider,
            retries: cloud_retries,
            retry_timeout_ms: cloud_retry_timeout_ms,
            retry_init_backoff_ms: cloud_retry_init_backoff_ms,
            retry_max_backoff_ms: cloud_retry_max_backoff_ms,
            cache_ttl: cloud_cache_ttl,
            keys: cloud_keys,
            values: cloud_values,
            len: cloud_len,
        };

        let rt = get_runtime();
        let mut total_optimized_files = 0;

        // 2. Initial Load
        let (mut table, _polars_schema) = rt.block_on(async {
            let t = DeltaTable::try_from_url_with_storage_options(
                table_url.clone(), 
                delta_storage_options.clone()
            )
            .await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;
            
            let s = get_polars_schema_from_delta(&t)?;
            Ok::<_, PolarsError>((t, s))
        })?;

        // =========================================================================
        // THE GRAND RETRY LOOP
        // =========================================================================
        let mut attempt = 0;
       
        loop {
            attempt += 1;
            
            // Reload table if retrying
            if attempt > 1 {
                println!("[Delta-RS] Conflict detected (Optimize). Retry attempt {}...", attempt);
                rt.block_on(async {
                    table.update_state().await
                        .map_err(|e| PolarsError::ComputeError(format!("Reload table failed: {}", e).into()))
                })?;
            }

            // Phase 1: Plan
            let bins = rt.block_on(phase_1_plan_bins(&ctx, &table))?;
            
            if bins.is_empty() {
                break; // No more files to optimize
            }

            // Phase 2 & 3: Execute & Commit per Bin
            let mut loop_success = true;

            for bin in bins {
                // Execute Rewrite
                let (staging_dir, write_id) = phase_2_execute_rewrite(&ctx, &table,&bin, &cloud_args)?;
                let staging_dir_fail_safe = staging_dir.clone();

                // Commit
                let commit_res = rt.block_on(
                    phase_3_commit_optimize(&mut table, &bin, &staging_dir, write_id)
                );

                match commit_res {
                    Ok(_) => {
                        total_optimized_files += bin.files.len();
                        // Update table state
                        rt.block_on(async {
                            table.update_state().await
                        }).map_err(|e| PolarsError::ComputeError(format!("Reload table failed: {}", e).into()))?;
                    },
                    Err(e) => {
                        // Handle Conflict
                        let err_msg = format!("{:?}", e);
                        let is_conflict = err_msg.contains("Transaction") || err_msg.contains("Conflict");
                        
                        // Cleanup Staging (if not cleaned inside)
                        rt.block_on(async {
                            let os = table.object_store();
                            let _ = os.delete(&Path::from(staging_dir_fail_safe)).await;
                        });

                        if is_conflict {
                            loop_success = false;
                            break; // Break inner loop, trigger outer loop retry logic
                        } else {
                            return Err(e); // Fatal error
                        }
                    }
                }
            }

            if loop_success {
                break; // All bins processed successfully
            }

            // Retry Backoff
            if attempt >= 5 {
                 return Err(PolarsError::ComputeError("Max retries exceeded for optimization".into()));
            }
            
            let backoff = std::cmp::min(100 * 2_u64.pow(attempt - 1), 2000);
            std::thread::sleep(std::time::Duration::from_millis(backoff));
        }

        if !out_num_files_optimized.is_null() {
            unsafe { *out_num_files_optimized = total_optimized_files; }
        }

        Ok(())
    })
}