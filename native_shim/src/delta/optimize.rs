use std::{collections::HashMap, ffi::c_char, time::{SystemTime, UNIX_EPOCH}};
use futures::StreamExt;
use polars::prelude::{file_provider::HivePathProvider, *};
use polars_buffer::Buffer;
use url::Url;
use deltalake::{DeltaTable, Path, kernel::scalars::ScalarExt};
use deltalake::kernel::{Action, Add, Remove, transaction::CommitBuilder};
use deltalake::protocol::DeltaOperation;
use uuid::Uuid;

use crate::{delta::{merge::phase_process_staging, utils::*}, utils::ptr_to_vec_string};
use crate::io::{build_cloud_options, build_parquet_write_options, build_unified_sink_args}; 
use crate::utils::{ptr_to_str};

// =========================================================
// 0. Context Definition
// =========================================================

struct OptimizeContext {
    pub table_url: Url,
    pub storage_options: HashMap<String, String>,
    pub target_size_bytes: i64,
    pub partition_filters: Option<HashMap<String, String>>,// Optional filter
    pub z_order_columns: Option<Vec<String>>
}

/// 一个“箱子”，代表一次具体的合并任务
#[derive(Debug, Clone)]
struct OptimizeBin {
    pub partition_values: HashMap<String, Option<String>>, // 分区键值对
    pub files: Vec<Add>,   // 需要被合并的碎片文件 (将会变成 Remove Actions)
    pub total_size: i64,   // 当前箱子总大小
}

// =========================================================
// Phase 1: Analysis (Bin-packing Strategy)
// =========================================================

/// 扫描全表，制定“装箱”计划
/// 返回：一组需要执行的“箱子”
async fn phase_1_plan_bins(
    ctx: &OptimizeContext,
    table: &DeltaTable,
) -> PolarsResult<Vec<OptimizeBin>> {
    
    // 1. 获取所有活跃文件流
    // 使用 get_active_add_actions_by_partitions 获取 LogicalFileView/Add
    // 这里传入空 slice 表示扫描全表（或者根据 ctx.partition_filter 构造过滤器）
    let mut stream = table.get_active_add_actions_by_partitions(&[]);

    // 2. 分组缓冲区: Key = Canonical Partition String
    let mut buckets: HashMap<String, Vec<Add>> = HashMap::new();

    let min_rewrite_threshold = ctx.target_size_bytes / 2;

    while let Some(view_res) = stream.next().await {
        let view = view_res.map_err(|e| PolarsError::ComputeError(format!("Delta stream error: {}", e).into()))?;
        
        // =========================================================
        // FIX: Manual conversion from LogicalFileView to Add
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
        // FIX: Manual conversion to Add Action
        // =========================================================
        let add = Add {
            path: view.path().to_string(),
            size: view.size(),
            partition_values: partition_values_map, // 使用手动提取的 Map
            modification_time: view.modification_time(),
            data_change: false, 
            stats: view.stats().map(|s| s.to_string()), 
            
            // Explicitly set to None as requested
            tags: None,
            deletion_vector: None,
            
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        if add.size >= min_rewrite_threshold {
            continue;
        }

        // =========================================================
        // FIX: 应用 Partition Filter
        // =========================================================
        if let Some(filters) = &ctx.partition_filters {
            let mut matched = true;
            for (key, target_val) in filters {
                // 从文件的分区值里找
                // 注意：partition_values 是 HashMap<String, Option<String>>
                if let Some(file_val_opt) = add.partition_values.get(key) {
                    // 如果文件里的值是 Some(val)，则比对；如果是 None (null)，则不匹配 (除非 filter 显式传 null，这里简化处理)
                    if let Some(file_val) = file_val_opt {
                        if file_val != target_val {
                            matched = false;
                            break;
                        }
                    } else {
                        // 文件分区值为 null，不匹配具体值
                        matched = false;
                        break;
                    }
                } else {
                    // 文件根本没有这个分区列（可能是 Schema 演变或非分区表），视为不匹配
                    matched = false;
                    break;
                }
            }
            if !matched { continue; }
        }

        // C. 生成分组 Key
        let part_key = if add.partition_values.is_empty() {
            "__unpartitioned__".to_string()
        } else {
            // 排序 key 以保证唯一性
            let mut keys: Vec<&String> = add.partition_values.keys().collect();
            keys.sort();
            keys.iter().map(|k| {
                format!("{}={}", k, add.partition_values.get(*k).unwrap_or(&Some("null".into())).as_deref().unwrap_or("null"))
            }).collect::<Vec<_>>().join("/")
        };

        buckets.entry(part_key).or_default().push(add);
    }

    // 3. 执行 Bin-packing (贪婪算法)
    let mut final_tasks = Vec::new();

    let max_bin_size = (ctx.target_size_bytes as f64 * 1.2) as i64;
    
    for (_part_key, mut files) in buckets {
        if files.is_empty() { continue; }

        files.sort_by_key(|f| f.size);
        
        // [FIX 1] 预先提取 Partition Values
        // 同一个 Bucket 下的所有文件分区键都是一样的，取第一个就行
        let partition_values = files[0].partition_values.clone();

        let mut current_bin_files = Vec::new();
        let mut current_bin_size = 0;

        for file in files {
            // [FIX 2] 找回丢失的 1.2 倍弹性阈值逻辑
            // 贪婪算法：如果加上当前文件会显著超过目标大小，就封箱
            if current_bin_size > 0 && (current_bin_size + file.size) > max_bin_size {
                
                // [FIX 3] 单文件跳过逻辑 (Write Amplification Check)
                // 只有当箱子里有 >1 个文件时，合并才有意义。
                if current_bin_files.len() > 1 {
                    final_tasks.push(OptimizeBin {
                        partition_values: partition_values.clone(), // Clone Map
                        files: std::mem::take(&mut current_bin_files), // 移走所有权，清空原 Vec
                        total_size: current_bin_size,
                    });
                } else {
                    // 即使不生成 Task，也要清空当前状态，开始新的箱子
                    // 否则这个大文件会和下一个文件粘连
                    current_bin_files.clear();
                }
                
                current_bin_size = 0;
            }

            current_bin_size += file.size;
            current_bin_files.push(file);
        }

        // [FIX 4] 处理残留的最后一个箱子 (Residual Bin)
        if !current_bin_files.is_empty() {
            // 同样的逻辑：如果只剩 1 个文件，且之前没合并进任何东西，就不动它
            if current_bin_files.len() > 1 {
                final_tasks.push(OptimizeBin {
                    partition_values: partition_values, // Move (最后一次用了，不用 Clone)
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
    bin: &OptimizeBin,
    cloud_args: &RawCloudArgs,
    schema: &Schema, // Delta Table 的 Schema (转为 Polars Schema)
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
    // 2. Construct Reader (LazyFrame)
    // -----------------------------------------------------
    // 将 Add Actions 转换为 Polars 路径
    let full_paths: Vec<PlRefPath> = bin.files.iter()
        .map(|f| {
            let full = format!("{}/{}", root_trimmed, f.path);
            PlRefPath::new(full)
        })
        .collect();
    
    let paths_buffer = Buffer::from(full_paths);

    // 复用 merge.rs 中的构建逻辑
    let mut scan_args = ScanArgsParquet::default();
    scan_args.cloud_options = unsafe {
        build_cloud_options(
            cloud_args.provider, cloud_args.retries, cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms, cloud_args.retry_max_backoff_ms, cloud_args.cache_ttl,
            cloud_args.keys, cloud_args.values, cloud_args.len,
        )
    };
    // 强制 Schema 检查，防止类型推断错误
    scan_args.schema = Some(Arc::new(schema.clone()));
    scan_args.low_memory = true; // 优化操作通常涉及大IO
    scan_args.rechunk = false;   // 流式处理不需要

    let mut lf = LazyFrame::scan_parquet_files(paths_buffer, scan_args)?;

    if let Some(z_cols) = &ctx.z_order_columns {
        // 调用我们刚写好的硬核 Z-Order 模块
        lf = crate::delta::zorder::apply_z_order(lf, z_cols)?;
    }

    // 3. Construct Writer (Sink)
    // -----------------------------------------------------
    // 配置写入选项
    let write_opts = build_parquet_write_options(
        1,  // compression: Snappy
        -1, // level
        true, // statistics (Crucial for Delta!)
        0, 0, -1
    ).map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;

    let unified_args = unsafe {
        build_unified_sink_args(
            true, // mkdir
            false, // maintain_order
            0,
            cloud_args.provider, cloud_args.retries, cloud_args.retry_timeout_ms,
            cloud_args.retry_init_backoff_ms, cloud_args.retry_max_backoff_ms, cloud_args.cache_ttl,
            cloud_args.keys, cloud_args.values, cloud_args.len,
        )
    };

    // 配置 Destination
    // 由于我们是按 Bin (单个分区) 处理的，我们可以直接写到 Staging 根目录，
    // 稍后 phase_process_staging 会处理重命名。
    // 但是，为了兼容 HivePathProvider，我们还是设置一下
    let hive_provider = HivePathProvider {
        extension: PlSmallStr::from_str(".parquet"),
    };

    let destination = SinkDestination::Partitioned {
        base_path: PlRefPath::new(&staging_uri),
        file_path_provider: Some(file_provider::FileProviderType::Hive(hive_provider)),
        // Optimize 不应该再重新分区，因为数据已经是按分区读出来的
        partition_strategy: PartitionStrategy::FileSize, 
        max_rows_per_file: u32::MAX, // 尽量写大文件
        approximate_bytes_per_file: usize::MAX as u64,
    };

    // 4. Execute Flow
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
    
    // 1. Promote Staging Files (复用 merge.rs 中的函数!)
    // 这个函数非常强大，它处理了 List -> Read Stats -> Rename -> Build Add 的全过程
    // 我们需要把 partition cols 的 key 提取出来
    let partition_cols: Vec<String> = bin.partition_values.keys().cloned().collect();
    
    // 注意：这里的 phase_process_staging 必须是你 merge.rs 里定义的那个
    let new_add_actions = phase_process_staging(
        table, 
        staging_dir, 
        &partition_cols, 
        write_id
    ).await?;

    if new_add_actions.is_empty() {
        // 如果没有生成新文件（可能是空数据），清理并返回
        let object_store = table.object_store();
        let _ = object_store.delete(&Path::from(staging_dir)).await;
        return Ok(());
    }

    // 2. Construct Remove Actions (Source files)
    let remove_actions: Vec<Action> = bin.files.iter().map(|f| {
        Action::Remove(Remove {
            path: f.path.clone(),
            deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
            data_change: false, // Optimize 不改变数据逻辑
            extended_file_metadata: Some(true),
            partition_values: Some(f.partition_values.clone()),
            size: Some(f.size),
            ..Default::default()
        })
    }).collect();

    // 3. Combine Actions
    let mut actions = Vec::with_capacity(new_add_actions.len() + remove_actions.len());
    actions.extend(remove_actions);
    actions.extend(new_add_actions);

    // 4. Commit
    let operation = DeltaOperation::Optimize {
        target_size: 0, // 仅作记录用
        predicate: None,
    };

    let commit_res = CommitBuilder::default()
        .with_actions(actions)
        .with_max_retries(0) // 严格模式，外部控制重试
        .build(
            Some(table.snapshot().map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?), 
            table.log_store().clone(),
            operation
        )
        .await;

    // 5. Cleanup Staging (无论成功失败最好都清理，但这里遵循 merge.rs 在 commit 后清理)
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
    // [NEW] Z-Order 参数
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
                // 解析为简单的 Key-Value Map
                // 例如: {"date": "2024-01-01", "region": "us"}
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
            storage_options: delta_storage_options,
            target_size_bytes: target_size_mb * 1024 * 1024,
            partition_filters:partition_filters, 
            z_order_columns,// 存入 Context
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
        let (mut table, _, polars_schema) = rt.block_on(async {
            let t = DeltaTable::try_from_url_with_storage_options(ctx.table_url.clone(), ctx.storage_options.clone())
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;
            let s = get_polars_schema_from_delta(&t)?;
            Ok::<_, PolarsError>((t, (), s))
        })?;

        // =========================================================================
        // THE GRAND RETRY LOOP
        // =========================================================================
        let mut attempt = 0;
        // Optimize 是一个长运行任务，通常包含多个 Bin。
        // 为了原子性和简单性，我们这里采用 "One-Shot Plan" 策略：
        // 制定好计划后，逐个 Bin 执行。如果某个 Bin 提交失败（冲突），则重试该 Bin（或者重载表重新规划）。
        
        // 这里的逻辑稍微调整：我们在 Loop 内部进行 Plan，这样每次重试都能基于最新状态。
        
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
            // 注意：为了减少冲突概率，我们一个个 Bin 提交。
            // 如果中间失败，外层 Loop 会重试整个过程（重新 Plan 剩余的文件）。
            let mut loop_success = true;

            for bin in bins {
                // Execute Rewrite
                let (staging_dir, write_id) = phase_2_execute_rewrite(&ctx, &bin, &cloud_args, &polars_schema)?;
                let staging_dir_fail_safe = staging_dir.clone();

                // Commit
                let commit_res = rt.block_on(
                    phase_3_commit_optimize(&mut table, &bin, &staging_dir, write_id)
                );

                match commit_res {
                    Ok(_) => {
                        total_optimized_files += bin.files.len();
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