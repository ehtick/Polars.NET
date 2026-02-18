use std::{collections::HashMap, ffi::c_char, time::{SystemTime, UNIX_EPOCH}};

use chrono::Utc;
use deltalake::{DeltaTable,Path}; 
use deltalake::kernel::{Action, Add, Remove};
use deltalake::kernel::scalars::ScalarExt;
use deltalake::kernel::transaction::{self, CommitBuilder};
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use deltalake::protocol::DeltaOperation;
use futures::StreamExt;
use polars::{error::{PolarsError, PolarsResult}, frame::DataFrame, prelude::{DataType, LazyFrame, PlRefPath, ScanArgsParquet}, series::Series};
use polars::prelude::*;
use uuid::Uuid;
use crate::delta::utils::{build_delta_storage_options_map, check_file_overlap_optimized, extract_bounds_from_expr, extract_delta_stats, get_polars_schema_from_delta, get_runtime, parse_hive_partitions, parse_table_url};
use crate::io::{build_cloud_options, build_parquet_write_options, build_unified_sink_args};
use crate::types::ExprContext;
use crate::utils::ptr_to_str;

#[unsafe(no_mangle)]
pub extern "C" fn pl_io_delta_delete(
    table_path_ptr: *const c_char,
    predicate_ptr: *mut ExprContext, 
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
        // 1. Arg Parsing
        let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
        // Take ownership of Expr
        let predicate_ctx = unsafe { *Box::from_raw(predicate_ptr) };

        let root_expr = &predicate_ctx.inner;
        let mut key_bounds = HashMap::new();
        extract_bounds_from_expr(root_expr, &mut key_bounds);

        let rt = get_runtime();

        // 2. Load Table & Get Active Actions (Sync Block)
        let (table, polars_schema, partition_cols, active_add_actions) = rt.block_on(async {
            // 2.1 Load Table
            let t = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;
            let scan_t = t.clone();
            // 2.2 Get Schema & Partition Cols from Snapshot
            let snapshot = scan_t.snapshot().map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
            let schema = get_polars_schema_from_delta(&t)?;
            let metadata = snapshot.metadata();
            let part_cols = metadata.partition_columns().clone();

            // 2.3 Get Actions
            let mut stream = scan_t.get_active_add_actions_by_partitions(&[]);
            let mut actions = Vec::new();
            
            while let Some(item) = stream.next().await {
                // Unwrap DeltaResult<LogicalFileView>
                let view = item.map_err(|e| PolarsError::ComputeError(format!("Stream error: {}", e).into()))?;
                actions.push(view);
            }

            Ok::<_, PolarsError>((t, schema, part_cols, actions))
        })?;

        // // 3. Get actions

        let mut actions = Vec::new();
        let object_store = table.object_store(); 
        let table_root = table.table_url().to_string(); 

        for view in active_add_actions {
            let file_path_str = view.path().clone(); 
            
            let partition_struct_opt = view.partition_values();
            let (p_fields, p_values) = match &partition_struct_opt {
                Some(sd) => (sd.fields(), sd.values()),
                None => (&[][..], &[][..]), 
            };
            // =================================================================================
            // PHASE 1: Partition Pruning (The "Fast Drop" Check)
            // =================================================================================
            // Try build a single row DataFrame to predicate
            let mut can_skip_scan = false;
            let mut fast_drop = false;

            if !partition_cols.is_empty() {
                // Build Mini-DataFrame from partition values
                let mut columns = Vec::with_capacity(partition_cols.len());
                for target_col in &partition_cols {
                    // Find Partition value from actions
                    let val_str_opt = p_fields.iter()
                        .position(|f| f.name() == target_col) // Get index
                        .map(|idx| &p_values[idx])            // Get Scalar
                        .filter(|scalar| !scalar.is_null())   // Filter Null
                        .map(|scalar| {
                             scalar.serialize()
                        });

                    let dtype = polars_schema.get_field(target_col).map(|f| f.dtype.clone()).unwrap_or(DataType::String);
                    
                    // Build Series
                    let s = match val_str_opt {
                        Some(v) => Series::new(target_col.into(), &[v]).cast(&dtype)?,
                        None => Series::new_null(target_col.into(), 1).cast(&dtype)?
                    };
                    columns.push(s.into());
                }
                
                if let Ok(mini_df) = DataFrame::new(1,columns) {
                    // Try to evaluate predicate
                    // Clone expr because we might need it later if this fails
                    let eval_result = mini_df.lazy()
                        .select([predicate_ctx.inner.clone().alias("result")])
                        .collect_with_engine(Engine::Streaming);

                    match eval_result {
                        Ok(res) => {
                            // [Success]: Predicate only depends on Partition Columns!
                            // Check the boolean result
                            if let Ok(bool_s) = res.column("result") {
                                if let Ok(is_match) = bool_s.bool() {
                                    if is_match.get(0) == Some(true) {
                                        // Partition Matched Predicate -> Drop entire file
                                        fast_drop = true;
                                    } else {
                                        // Partition Did NOT Match -> Keep entire file (No-op)
                                        // Do nothing for this file loop
                                    }
                                    can_skip_scan = true;
                                }
                            }
                        },
                        Err(_e) => {
                            // [Failure]: Error likely means "ColumnNotFound".
                            // This confirms the Predicate involves Data Columns (e.g. "id").
                            // We MUST proceed to scan the file content.
                            // Ignore error and fall through to Phase 2.
                        }
                    }
                }
            }

            // --- Apply Pruning Result ---
            if can_skip_scan {
                if fast_drop {
                    // Fast Drop: Mark file for removal, skip IO
                    let remove = Remove {
                        path: file_path_str.to_string().clone(),
                        deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                        data_change: true,
                        ..Default::default()
                    };
                    actions.push(Action::Remove(remove));
                }
                // If fast_drop is false, we simply keep the file (do nothing), effectively skipping it.
                continue; 
            }

            // =========================================================
             // Phase 1.5: Stats Pruning (Auto-Derived)
             // =========================================================
            let stats_struct = view.stats()
                    .map(|s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null));
             if !key_bounds.is_empty() {

                 let mut file_overlaps = true;
                 for (key, (src_min, src_max)) in &key_bounds {
                     if !check_file_overlap_optimized(&stats_struct, key, src_min, src_max) {
                         file_overlaps = false;
                         break;
                     }
                 }
                 
                 if !file_overlaps {
                     continue;
                 }
            }

            // =================================================================================
            // PHASE 2: Scan & Filter (Stats Pushdown Optimized)
            // =================================================================================

            let total_rows_opt = stats_struct.as_ref()
                .and_then(|v| v.get("numRecords"))
                .and_then(|v| v.as_i64());
            
            let full_scan_path = format!("{}/{}", table_root.trim_end_matches('/'), file_path_str);
            let scan_cloud_options = unsafe {
                build_cloud_options(
                    cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                    cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                    cloud_keys, cloud_values, cloud_len
                )
            };
            
            let mut lf = LazyFrame::scan_parquet(
                PlRefPath::new(full_scan_path.clone()), 
                ScanArgsParquet { cloud_options: scan_cloud_options, ..Default::default() }
            )?;

            // Inject Partitions
            for target_col in &partition_cols {
                // Find Value
                let val_str_opt = p_fields.iter()
                    .position(|f| f.name() == target_col)
                    .map(|idx| &p_values[idx])
                    .filter(|scalar| !scalar.is_null())
                    .map(|scalar| {
                         scalar.serialize()
                    });

                // Build Literal Expr
                let lit_expr = match val_str_opt {
                    Some(v) => lit(v),
                    None => lit(NULL)
                };

                // Cast and Inject
                let final_expr = if let Some(dtype) = polars_schema.get_field(target_col).map(|f| f.dtype.clone()) {
                    lit_expr.cast(dtype)
                } else {
                    lit_expr
                };
                
                lf = lf.with_column(final_expr.alias(target_col));
            }

            // [Optimization]: Use filter() first to allow Parquet Stats Pushdown
            // If Parquet Min/Max stats prove "No Match", filter returns empty LF instantly.
            let has_match_df = lf.clone()
                .filter(root_expr.clone())
                .limit(1)
                .collect_with_engine(Engine::Streaming)?;
            
            if has_match_df.height() == 0 {
                // Case 1: Keep (Predicate missed)
                continue;
            }
            
            let is_full_drop = if let Some(_total) = total_rows_opt {
                let keep_expr = root_expr.clone().not();
                let has_keep_df = lf.clone()
                    .filter(keep_expr)
                    .limit(1) 
                    .collect_with_engine(Engine::Streaming)?;
                
                has_keep_df.height() == 0
            } else {
                let counts_df = lf.clone()
                    .select([
                        len().alias("total"), 
                        root_expr.clone().cast(DataType::UInt32).sum().alias("matched") 
                    ])
                    .collect_with_engine(Engine::Streaming)?;

                let total = counts_df.column("total")?.u32()?.get(0).unwrap_or(0) as i64;
                let matched = counts_df.column("matched")?.u32()?.get(0).unwrap_or(0) as i64;

                // if matched == total => Drop
                matched == total
            };

            if is_full_drop {
                // Case 2: Drop (Full match)
                let remove = Remove {
                    path: file_path_str.to_string().clone(),
                    deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                    data_change: true,
                    ..Default::default()
                };
                actions.push(Action::Remove(remove));
            } else {
                // Case 3: Rewrite (Partial match)
                let remove = Remove {
                    path: file_path_str.to_string().clone(),
                    deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                    data_change: true,
                    ..Default::default()
                };
                actions.push(Action::Remove(remove));

                // Rewrite: Keep rows that do NOT match
                let keep_expr = predicate_ctx.inner.clone().not();
                
                // Construct Selector to drop partition cols
                let drop_names: Vec<PlSmallStr> = partition_cols.iter()
                    .map(|s| PlSmallStr::from_string(s.clone()))
                    .collect();
                let drop_selector = Selector::ByName { names: drop_names.into(), strict: true };

                let new_lf = lf.filter(keep_expr).drop(drop_selector);

                let new_name = format!("part-{}-{}.parquet", Utc::now().timestamp_millis(), Uuid::new_v4());
                let parent_dir = std::path::Path::new(file_path_str.as_ref()) 
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| "".to_string());
                let relative_new_path = if parent_dir.is_empty() { new_name.clone() } else { format!("{}/{}", parent_dir, new_name) };
                let full_write_path = format!("{}/{}", table_root.trim_end_matches('/'), relative_new_path);

                let write_opts = build_parquet_write_options(1, -1, true, 0, 0, -1)
                    .map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;
                
                let target = SinkTarget::Path(PlRefPath::new(&full_write_path));
                let unified_args = unsafe {
                    build_unified_sink_args(
                        false, false, 0, cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                        cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
                        cloud_keys, cloud_values, cloud_len 
                    )
                };

                new_lf.sink(SinkDestination::File { target }, FileWriteFormat::Parquet(write_opts), unified_args)?
                    .collect_with_engine(Engine::Streaming)?;

                // Read Stats & Add Action
                let (file_size, stats_json) = rt.block_on(async {
                    let path = Path::from(relative_new_path.clone());
                    let meta = object_store.head(&path).await
                         .map_err(|e| PolarsError::ComputeError(format!("Head error: {}", e).into()))?;
                    let mut reader = ParquetObjectReader::new(object_store.clone(), path.clone())
                            .with_file_size(meta.size as u64);
                    let footer = reader.get_metadata(None).await
                            .map_err(|e| PolarsError::ComputeError(format!("Footer error: {}", e).into()))?;
                    let (_, json) = extract_delta_stats(&footer)?;
                    Ok::<_, PolarsError>((meta.size as i64, json))
                })?;

                let new_partition_values = parse_hive_partitions(&relative_new_path, &partition_cols);
                let add = Add {
                    path: relative_new_path,
                    size: file_size,
                    partition_values: new_partition_values,
                    modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
                    data_change: true,
                    stats: Some(stats_json),
                    ..Default::default()
                };
                actions.push(Action::Add(add));
            }
        }
        // 4. Commit
        if !actions.is_empty() {
            rt.block_on(async {
                let operation = DeltaOperation::Delete {
                    predicate: None, 
                };
                
                let _ver = CommitBuilder::default()
                    .with_actions(actions)
                    .build(
                        table.snapshot().ok().map(|s| s as &dyn transaction::TableReference),
                        table.log_store().clone(),
                        operation
                    ).await.map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
                
                Ok::<(), PolarsError>(())
            })?;
        }
        Ok(())
    })
}