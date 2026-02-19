use std::{collections::HashMap, ffi::c_char, time::{SystemTime, UNIX_EPOCH}};

use chrono::Utc;
use deltalake::{DeltaTable, ObjectStore, kernel::{DeletionVectorDescriptor, StorageType}, table::state::DeltaTableState}; 
use deltalake::kernel::{Action, Add, Remove};
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use deltalake::protocol::DeltaOperation;
use futures::StreamExt;
use polars::{error::{PolarsError, PolarsResult}, frame::DataFrame, prelude::{DataType, LazyFrame, PlRefPath, ScanArgsParquet}, series::Series};
use polars::prelude::*;
use roaring::RoaringBitmap;
use uuid::Uuid;
use crate::delta::{deletion_vector::read_deletion_vector, utils::{RawCloudArgs, build_delta_storage_options_map, check_file_overlap_optimized, extract_delta_stats, get_polars_schema_from_delta, get_runtime, parse_hive_partitions, parse_table_url, view_to_add_action}};
use crate::io::{build_cloud_options, build_parquet_write_options, build_unified_sink_args};
use crate::types::ExprContext;
use crate::utils::ptr_to_str;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeleteStrategy {
    /// Copy-on-Write (CoW): Rewrite the entire Parquet file excluding deleted rows.
    /// Used when:
    /// 1. Table does not support deletion vectors.
    /// 2. User explicitly requests it (e.g., for compaction).
    CopyOnWrite,

    /// Merge-on-Read (MoR): Write a Deletion Vector (Bitmap) file.
    /// Used when:
    /// 1. Table supports deletion vectors (Reader v3, Writer v7).
    /// 2. 'deletionVectors' feature is enabled.
    MergeOnRead,
}

impl DeleteStrategy {
    /// 根据表协议和特性自动决定策略
    /// [FIXED] 参数类型改为 &DeltaTableState 以匹配 table.snapshot() 的返回值
    pub fn determine(state: &DeltaTableState, force_cow: bool) -> Self {
        if force_cow {
            return Self::CopyOnWrite;
        }

        // DeltaTableState 直接暴露了 protocol() 方法
        let protocol = state.protocol();
        
        // 判断 Writer Version >= 7 (DV 要求)
        if protocol.min_writer_version() >= 7 {
            // TODO: 未来还可以检查 state.current_metadata() 里的 configuration 
            // 确认是否开启了 delta.enableDeletionVectors
            Self::MergeOnRead
        } else {
            Self::CopyOnWrite
        }
    }
}

/// DeleteContext holds all the necessary state for a delete operation.
/// It decouples the execution logic from the FFI interface.
pub struct DeleteContext {
    // --- Identity & Config ---
    pub table_root: String,
    pub strategy: DeleteStrategy,
    
    // --- Logic ---
    pub predicate: Expr, // The WHERE clause
    pub key_bounds: HashMap<String, (Option<serde_json::Value>, Option<serde_json::Value>)>,
    
    // --- Schema Information ---
    pub polars_schema: Schema,
    pub partition_cols: Vec<String>,
    
    // --- Infrastructure ---
    pub object_store: Arc<dyn ObjectStore>,
    pub cloud_args: RawCloudArgs, // For Polars Scan/Sink (FFI compat)
    
    // --- Runtime ---
    // (Optional) We might need a reference to the runtime if we do deep async calls
    // but usually passing object_store is enough.
}

impl DeleteContext {
    pub fn new(
        table: &DeltaTable,
        predicate: Expr,
        polars_schema: Schema,
        partition_cols: Vec<String>,
        strategy: DeleteStrategy,
        cloud_args: RawCloudArgs,
    ) -> Self {
        // Extract Key Bounds immediately upon creation
        let mut key_bounds = HashMap::new();
        crate::delta::utils::extract_bounds_from_expr(&predicate, &mut key_bounds);

        Self {
            table_root: table.table_url().to_string(),
            strategy,
            predicate,
            key_bounds,
            polars_schema,
            partition_cols,
            object_store: table.object_store(),
            cloud_args,
        }
    }
}

/// Represents the decision made after analyzing a file (Pruning Phase).
pub enum FileActionDecision {
    /// 1. Skip: The file does not contain any data matching the predicate.
    /// (Based on Partition Pruning or Stats Pruning)
    Skip,

    /// 2. Full Drop: Every row in this file matches the predicate.
    /// We can simply emit a `Remove` action for this file.
    /// (Optimization: No need to rewrite or write DV)
    FullDrop(Action), // Action::Remove

    /// 3. Process: The file *might* contain matching rows.
    /// We need to Scan it and apply the Delete Strategy (CoW or MoR).
    Process {
        file_view: Add, // The original file info
    },
}

/// 将 RoaringBitmap 序列化并写入存储
/// 返回: (文件名, 文件大小)
async fn write_dv_file(
    ctx: &DeleteContext,
    bitmap: &RoaringBitmap,
) -> PolarsResult<(String, i64)> {
    // 1. 序列化 Bitmap (Portable Format)
    // Delta Lake 要求使用 RoaringBitmap 的 "Portable" 格式
    let mut buf = Vec::new();
    bitmap.serialize_into(&mut buf)
        .map_err(|e| PolarsError::ComputeError(format!("Failed to serialize bitmap: {}", e).into()))?;

    // 2. 生成文件名
    // 格式: deletion_vector_{uuid}.bin
    let uuid = Uuid::new_v4();
    let file_name = format!("deletion_vector_{}.bin", uuid);
    
    // 3. 构建路径
    // DV 文件通常和数据文件在同一个表目录下 (或者专门的文件夹，但根目录最常见)
    let path = deltalake::Path::from(format!("{}/{}", ctx.table_root.trim_end_matches('/'), file_name));

    // 4. 写入存储
    ctx.object_store.put(&path, buf.clone().into()).await
        .map_err(|e| PolarsError::ComputeError(format!("Failed to write DV file: {}", e).into()))?;

    Ok((file_name, buf.len() as i64))
}

/// 生成符合 Delta Protocol 的 DV 描述符
fn create_dv_descriptor(
    file_name: &str, // deletion_vector_{uuid}.bin
    dv_size: i64,
    cardinality: i64,
) -> PolarsResult<DeletionVectorDescriptor> {
    
    // 1. 从文件名提取 UUID
    // file_name: "deletion_vector_xxxxxxxx-xxxx-....bin"
    let uuid_str = file_name
        .trim_start_matches("deletion_vector_")
        .trim_end_matches(".bin");
    
    let uuid = Uuid::parse_str(uuid_str)
        .map_err(|e| PolarsError::ComputeError(format!("Invalid UUID in DV filename: {}", e).into()))?;

    // 2. 生成 Random Prefix (1 字节) + Base85(UUID)
    // Delta 规范：为了防止 S3 热点，UUID 前面要加个随机前缀。
    // pathOrInlineDv = <randomPrefix> + <base85(uuid)>
    // randomPrefix 长度是 `offset`。对于 'u' 类型，offset 通常是 1。
    
    // A. 编码 UUID (16 bytes -> 20 chars Z85)
    let uuid_bytes = uuid.as_bytes();
    let uuid_z85 = z85::encode(uuid_bytes);

    // B. 生成随机前缀 (1 char)
    // 随便搞个字符就行，比如 'x'，或者随机生成
    let prefix = "p"; 
    
    // C. 拼接
    let path_encoded = format!("{}{}", prefix, uuid_z85);

    Ok(DeletionVectorDescriptor {
        storage_type: StorageType::UuidRelativePath, // 'u'
        path_or_inline_dv: path_encoded,
        offset: Some(0), // 前缀长度为 0
        size_in_bytes: dv_size as i32,
        cardinality,
    })
}

/// Analyze a single file to determine if it can be skipped, fully dropped, or needs processing.
// pub fn prune_file(ctx: &DeleteContext, view: &LogicalFileView) -> PolarsResult<FileActionDecision> {
    
//     // =================================================================================
//     // PHASE A: Partition Pruning (分区裁剪)
//     // =================================================================================
//     if !ctx.partition_cols.is_empty() {
//         // 1. 获取 Partition Values (StructData)
//         // 这里的逻辑直接参考了你提供的源码实现
//         let partition_struct_opt = view.partition_values();
//         let (p_fields, p_values) = match &partition_struct_opt {
//             Some(sd) => (sd.fields(), sd.values()),
//             None => (&[][..], &[][..]), 
//         };

//         // 2. 构建 Mini-DataFrame (1行)
//         let mut columns = Vec::with_capacity(ctx.partition_cols.len());
        
//         for target_col in &ctx.partition_cols {
//             // [Copy from User Logic] 查找列索引并提取值
//             let val_str_opt = p_fields.iter()
//                 .position(|f| f.name() == target_col) // Find index by name
//                 .map(|idx| &p_values[idx])            // Get Scalar
//                 .filter(|scalar| !scalar.is_null())   // Filter Nulls
//                 .map(|scalar| {
//                      scalar.serialize()               // Convert to String
//                 });

//             // 获取 Polars 里的目标类型，以便 Cast
//             let dtype = ctx.polars_schema.get_field(target_col)
//                 .map(|f| f.dtype.clone())
//                 .unwrap_or(DataType::String);
            
//             // 构建 Series
//             // 注意：scalar.serialize() 返回的是 String，所以我们先创建 String Series，然后 Cast
//             let s = match val_str_opt {
//                 Some(v) => Series::new(target_col.into(), &[v]).cast(&dtype)?,
//                 None => Series::new_null(target_col.into(), 1).cast(&dtype)?
//             };
//             columns.push(s.into());
//         }

//         if let Ok(mini_df) = DataFrame::new(1,columns) {
//             // 3. 评估 Predicate
//             let eval_result = mini_df.lazy()
//                 .select([ctx.predicate.clone().alias("result")])
//                 .collect_with_engine(Engine::Streaming);

//             match eval_result {
//                 Ok(res) => {
//                     if let Ok(bool_s) = res.column("result") {
//                         if let Ok(is_match) = bool_s.bool() {
//                             if is_match.get(0) == Some(true) {
//                                 // [Match]: 分区完全匹配 -> 全文件删除
//                                 // 使用 LogicalFileView 自带的方法生成 Remove Action
//                                 let remove = view.remove_action(true);
//                                 return Ok(FileActionDecision::FullDrop(Action::Remove(remove)));
//                             } else {
//                                 // [No Match]: 分区完全不匹配 -> 跳过
//                                 return Ok(FileActionDecision::Skip);
//                             }
//                         }
//                     }
//                 },
//                 Err(_) => {
//                     // Predicate 涉及非分区列，无法在此阶段判断，继续。
//                 }
//             }
//         }
//     }

//     // =================================================================================
//     // PHASE B: Stats Pruning (统计信息裁剪)
//     // =================================================================================
//     if !ctx.key_bounds.is_empty() {
//         // LogicalFileView 提供了 stats() 方法返回 Option<String> (JSON)
//         let stats_struct = view.stats()
//             .map(|s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null));

//         let mut file_overlaps = true;
//         for (key, (src_min, src_max)) in &ctx.key_bounds {
//             if !check_file_overlap_optimized(&stats_struct, key, src_min, src_max) {
//                 file_overlaps = false;
//                 break;
//             }
//         }
        
//         if !file_overlaps {
//             return Ok(FileActionDecision::Skip);
//         }
//     }

//     // =================================================================================
//     // PHASE C: Pass to Process (进入执行阶段)
//     // =================================================================================
//     // 这里的 view 是借用的，我们需要把它转换成 Owned 的 Add 结构体传给后续流程。
//     // 使用 LogicalFileView 自带的 add_action() 方法，它会自动处理所有字段映射。
    
//     let add_action = view_to_add_action(view);

//     Ok(FileActionDecision::Process { file_view: add_action })
// }

/// Version of prune_file that works on Owned Add struct
pub fn prune_file_from_add(ctx: &DeleteContext, add: &Add) -> PolarsResult<FileActionDecision> {
    
    // 1. Partition Pruning
    if !ctx.partition_cols.is_empty() {
        // Build Mini-DataFrame from Add.partition_values (Map<String, Option<String>>)
        let mut columns = Vec::with_capacity(ctx.partition_cols.len());
        
        for target_col in &ctx.partition_cols {
            let val_opt = add.partition_values.get(target_col).and_then(|v| v.as_ref());
            
            let dtype = ctx.polars_schema.get_field(target_col)
                .map(|f| f.dtype.clone())
                .unwrap_or(DataType::String);
            
            let s = match val_opt {
                Some(v) => Series::new(target_col.into(), &[v.as_str()]).cast(&dtype)?,
                None => Series::new_null(target_col.into(), 1).cast(&dtype)?
            };
            columns.push(s.into());
        }

        if let Ok(mini_df) = DataFrame::new(1,columns) {
            let eval_result = mini_df.lazy()
                .select([ctx.predicate.clone().alias("result")])
                .collect_with_engine(Engine::Streaming);

            if let Ok(res) = eval_result {
                if let Ok(bool_s) = res.column("result") {
                    if bool_s.bool().ok().map(|b| b.get(0) == Some(true)).unwrap_or(false) {
                        // Match -> Full Drop
                        // 手动构建 Remove (Add -> Remove)
                        let remove = Remove {
                            path: add.path.clone(),
                            deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
                            data_change: true,
                            extended_file_metadata: Some(true),
                            partition_values: Some(add.partition_values.clone()),
                            size: Some(add.size),
                            deletion_vector: add.deletion_vector.clone(),
                            tags: None,
                            base_row_id: None,
                            default_row_commit_version: None,
                        };
                        return Ok(FileActionDecision::FullDrop(Action::Remove(remove)));
                    } else {
                        return Ok(FileActionDecision::Skip);
                    }
                }
            }
        }
    }

    // 2. Stats Pruning
    if !ctx.key_bounds.is_empty() {
        // Add.stats is Option<String> JSON
        let stats_struct = add.stats.as_ref()
            .map(|s| serde_json::from_str(s).unwrap_or(serde_json::Value::Null));

        let mut file_overlaps = true;
        for (key, (src_min, src_max)) in &ctx.key_bounds {
            if !check_file_overlap_optimized(&stats_struct, key, src_min.as_ref(), src_max.as_ref()) {
                file_overlaps = false;
                break;
            }
        }
        
        if !file_overlaps {
            return Ok(FileActionDecision::Skip);
        }
    }

    // 3. Process
    // Clone is cheap enough here compared to IO
    Ok(FileActionDecision::Process { file_view: add.clone() })
}


/// 执行 Copy-on-Write 策略：重写文件，过滤掉匹配的数据
pub fn execute_copy_on_write(
    ctx: &DeleteContext,
    file_action: &Add, // 原始文件的 Add Action
) -> PolarsResult<Vec<Action>> {
    
    let file_path_str = &file_action.path;
    let full_scan_path = format!("{}/{}", ctx.table_root.trim_end_matches('/'), file_path_str);

    // 1. 构建 Scan Args
    // 需要使用 unsafe 构建 cloud options，复用 context 里的 args
    let scan_cloud_options = unsafe {
        build_cloud_options(
            ctx.cloud_args.provider, 
            ctx.cloud_args.retries, 
            ctx.cloud_args.retry_timeout_ms,
            ctx.cloud_args.retry_init_backoff_ms, 
            ctx.cloud_args.retry_max_backoff_ms, 
            ctx.cloud_args.cache_ttl,
            ctx.cloud_args.keys, 
            ctx.cloud_args.values, 
            ctx.cloud_args.len
        )
    };
    
    // 2. Scan Parquet
    let mut lf = LazyFrame::scan_parquet(
        PlRefPath::new(&full_scan_path), 
        ScanArgsParquet { cloud_options: scan_cloud_options, ..Default::default() }
    )?;

    // 3. 注入分区列 (Inject Partition Columns)
    // 原始逻辑里是从 View 拿的，现在我们可以从 file_action.partition_values 拿
    for target_col in &ctx.partition_cols {
        // 从 map 中获取值
        let val_opt = file_action.partition_values.get(target_col).and_then(|v| v.as_ref());

        // 构建 Literal Expr
        let lit_expr = match val_opt {
            Some(v) => lit(v.as_str()),
            None => lit(NULL)
        };

        // Cast 到正确类型
        let final_expr = if let Some(dtype) = ctx.polars_schema.get_field(target_col).map(|f| f.dtype.clone()) {
            lit_expr.cast(dtype)
        } else {
            lit_expr
        };
        
        lf = lf.with_column(final_expr.alias(target_col));
    }

    // 4. 检查是否真的有数据需要删除 (Optimization)
    // 这里的逻辑是你之前的 Phase 2 优化：先 filter(predicate).limit(1) 看看有没有命中
    // 如果没命中，其实可以直接 Skip (返回空 Action 列表)，但这应该在 prune_file 里尽量做掉。
    // 这里我们再做一次双保险。
    
    let has_match_df = lf.clone()
        .filter(ctx.predicate.clone())
        .limit(1)
        .collect_with_engine(Engine::Streaming)?;
    
    if has_match_df.height() == 0 {
        // 没有行匹配 Predicate -> 不需要重写，文件保留
        return Ok(Vec::new());
    }

    // 5. 准备重写 (Rewrite)
    // 逻辑：保留那些 [不匹配] Predicate 的行
    let keep_expr = ctx.predicate.clone().not();
    
    // Drop 分区列 (写入 Parquet 时不需要包含分区列，因为它们在路径里)
    let drop_names: Vec<PlSmallStr> = ctx.partition_cols.iter()
        .map(|s| PlSmallStr::from_string(s.clone()))
        .collect();
    // 使用新版 Selector
    let drop_selector = Selector::ByName { names: drop_names.into(), strict: false }; // strict=false 以防某些分区列没被识别

    let new_lf = lf.filter(keep_expr).drop(drop_selector);

    // 6. 生成新文件名
    // 格式：part-timestamp-uuid.parquet
    let new_name = format!("part-{}-{}.parquet", Utc::now().timestamp_millis(), Uuid::new_v4());
    
    // 保持原来的目录结构 (如果有分区目录)
    let parent_dir = std::path::Path::new(file_path_str) 
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|| "".to_string());
        
    let relative_new_path = if parent_dir.is_empty() { 
        new_name.clone() 
    } else { 
        format!("{}/{}", parent_dir, new_name) 
    };
    
    let full_write_path = format!("{}/{}", ctx.table_root.trim_end_matches('/'), relative_new_path);

    // 7. 执行写入 (Sink)
    let write_opts = build_parquet_write_options(1, -1, true, 0, 0, -1)
        .map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;
    
    let target = SinkTarget::Path(PlRefPath::new(&full_write_path));
    
    // 重新构建 Unified Args (Unsafe)
    let unified_args = unsafe {
        build_unified_sink_args(
            false, false, 0, 
            ctx.cloud_args.provider, ctx.cloud_args.retries, ctx.cloud_args.retry_timeout_ms,
            ctx.cloud_args.retry_init_backoff_ms, ctx.cloud_args.retry_max_backoff_ms, ctx.cloud_args.cache_ttl,
            ctx.cloud_args.keys, ctx.cloud_args.values, ctx.cloud_args.len 
        )
    };

    new_lf.sink(SinkDestination::File { target }, FileWriteFormat::Parquet(write_opts), unified_args)?
        .collect_with_engine(Engine::Streaming)?;

    // 8. 读取新文件 Stats (Metadata)
    // 这步需要 IO，如果不想在 block_on 里调，可以将 execute_cow 设为 async
    // 但目前架构是同步 FFI 调 block_on，所以这里再嵌套一个 runtime block_on 是安全的
    let rt = get_runtime();
    let (file_size, stats_json) = rt.block_on(async {
        let path = deltalake::Path::from(relative_new_path.clone());
        let meta = ctx.object_store.head(&path).await
                .map_err(|e| PolarsError::ComputeError(format!("Head error: {}", e).into()))?;
        
        let mut reader = ParquetObjectReader::new(ctx.object_store.clone(), path.clone())
                .with_file_size(meta.size as u64);
        let footer = reader.get_metadata(None).await
                .map_err(|e| PolarsError::ComputeError(format!("Footer error: {}", e).into()))?;
        
        let (_, json) = extract_delta_stats(&footer)?;
        Ok::<_, PolarsError>((meta.size as i64, json))
    })?;

    // 9. 构造 Actions
    let mut actions = Vec::new();

    // A. Remove Old File
    // 使用 remove_action(true) 自动生成，data_change=true
    // 既然我们手里是 Add 结构体，我们需要手动转 Remove，或者利用 LogicalFileView 的逻辑
    // 这里手动构造 Remove 更直接
    let remove = Remove {
        path: file_path_str.clone(),
        deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
        data_change: true,
        extended_file_metadata: Some(true),
        partition_values: Some(file_action.partition_values.clone()),
        size: Some(file_action.size),
        deletion_vector: file_action.deletion_vector.clone(), // 必须带上旧的 DV (如果有)
        tags: None,
        base_row_id: None,
        default_row_commit_version: None,
    };
    actions.push(Action::Remove(remove));

    // B. Add New File
    let add = Add {
        path: relative_new_path.clone(),
        size: file_size,
        partition_values: parse_hive_partitions(&relative_new_path, &ctx.partition_cols), // 重新解析最稳
        modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
        data_change: true,
        stats: Some(stats_json),
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    };
    actions.push(Action::Add(add));

    Ok(actions)
}

pub fn execute_merge_on_read(
    ctx: &DeleteContext,
    file_action: &Add,
) -> PolarsResult<Vec<Action>> {
    let file_path_str = &file_action.path;
    let full_scan_path = format!("{}/{}", ctx.table_root.trim_end_matches('/'), file_path_str);

    // 1. Scan Parquet 获取要删除的行号
    // 重要：我们只需要 row_index，不需要读其他列（除非 predicate 需要）
    // 为了性能，Polars 会只读 predicate 涉及的列 + row_index
    
    let scan_cloud_options = unsafe {
        build_cloud_options(
            ctx.cloud_args.provider, 
            ctx.cloud_args.retries, 
            ctx.cloud_args.retry_timeout_ms,
            ctx.cloud_args.retry_init_backoff_ms, 
            ctx.cloud_args.retry_max_backoff_ms, 
            ctx.cloud_args.cache_ttl,
            ctx.cloud_args.keys, 
            ctx.cloud_args.values, 
            ctx.cloud_args.len
        )
    };

    let mut lf = LazyFrame::scan_parquet(
        PlRefPath::new(&full_scan_path), 
        ScanArgsParquet { cloud_options: scan_cloud_options, ..Default::default() }
    )?;

    // 注入分区列 (Predicate 可能依赖分区列)
    for target_col in &ctx.partition_cols {
        let val_opt = file_action.partition_values.get(target_col).and_then(|v| v.as_ref());
        let lit_expr = match val_opt {
            Some(v) => lit(v.as_str()),
            None => lit(NULL)
        };
        let final_expr = if let Some(dtype) = ctx.polars_schema.get_field(target_col).map(|f| f.dtype.clone()) {
            lit_expr.cast(dtype)
        } else {
            lit_expr
        };
        lf = lf.with_column(final_expr.alias(target_col));
    }

    // 2. 筛选出要删除的行号
    // 逻辑：filter(predicate) -> select(row_nr)
    // 这里的 row_index 必须和 Parquet 物理行号一致，scan_parquet 默认保证这一点
    let deleted_indices_df = lf
        .with_row_index("row_nr", None)
        .filter(ctx.predicate.clone()) // 筛选出要删的
        .select([col("row_nr")])
        .collect_with_engine(Engine::Streaming)?;

    if deleted_indices_df.height() == 0 {
        return Ok(Vec::new()); // 没命中，跳过
    }

    // 收集新删除的行号
    let new_deleted_indices: Vec<u32> = deleted_indices_df
        .column("row_nr")?
        .u32()?
        .into_no_null_iter()
        .collect();

    // 3. 处理旧 DV (Merge Logic)
    let rt = get_runtime(); // 需要 Runtime 做 IO
    let table_root_path = deltalake::Path::from(ctx.table_root.trim_end_matches('/'));

    let mut bitmap = if let Some(dv_desc) = &file_action.deletion_vector {
        // 如果之前有 DV，必须读出来合并
        rt.block_on(async {
            read_deletion_vector(ctx.object_store.clone(), dv_desc, &table_root_path).await
        })?
    } else {
        // 之前没 DV，创建新的
        RoaringBitmap::new()
    };

    // 合并：Old | New
    bitmap.extend(new_deleted_indices);

    // 4. 写入新 DV 文件 (IO)
    let (dv_file_name, dv_size) = rt.block_on(async {
        write_dv_file(ctx, &bitmap).await
    })?;

    // 5. 构造 Actions
    let mut actions = Vec::new();

    // A. Remove Old File State
    // 注意：这里移除的是“带旧 DV”的那个状态
    let remove = Remove {
        path: file_path_str.clone(),
        deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
        data_change: true,
        extended_file_metadata: Some(true),
        partition_values: Some(file_action.partition_values.clone()),
        size: Some(file_action.size),
        deletion_vector: file_action.deletion_vector.clone(), // 必须带上旧 DV
        tags: None,
        base_row_id: None,
        default_row_commit_version: None,
    };
    actions.push(Action::Remove(remove));

    // B. Add New File State (Same Path, New DV)
    // 这是 MoR 的精髓：Path 不变，DV 变了
    let new_dv_descriptor = create_dv_descriptor(
        &dv_file_name, 
        dv_size, 
        bitmap.len() as i64 // cardinality = 删除了多少行
    )?;

    let add = Add {
        path: file_path_str.clone(), // 路径不变！
        size: file_action.size,      // 大小不变 (指 Parquet 文件大小)
        partition_values: file_action.partition_values.clone(),
        modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
        data_change: true,
        stats: file_action.stats.clone(), // 统计信息不变 (或者需要修正 numRecords?)
        // Delta 规范：Stats 通常反映物理文件的状态。逻辑上的 numRecords 会减小，
        // 但 stats 里的 min/max/nulls 通常不更新，除非重写文件。
        // 读取端会自己 apply DV，所以这里保持原样通常是安全的。
        tags: None,
        deletion_vector: Some(new_dv_descriptor), // 新的 DV
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    };
    actions.push(Action::Add(add));

    Ok(actions)
}

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
        // =================================================================================
        // Step 0: 参数解析 (Argument Parsing)
        // =================================================================================
        let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let table_url = parse_table_url(path_str)?;
        
        // Take ownership of Predicate Expression
        let predicate_ctx = unsafe { *Box::from_raw(predicate_ptr) };
        let predicate_expr = predicate_ctx.inner;

        // Pack RawCloudArgs for later use in Context
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
        
        // Build options for Delta-RS loading
        let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);

        let rt = get_runtime();

        // =================================================================================
        // Step 1: 加载表与获取文件 (Load Phase - Async)
        // =================================================================================
        // 我们在这里一次性把 View 转换为 Owned Add 结构体，避免后续生命周期问题
        let (table, polars_schema, partition_cols, all_files) = rt.block_on(async {
            // 1.1 Load Table
            let t = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options)
                .await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;
            
            // 1.2 Get Schema & Metadata
            let schema = get_polars_schema_from_delta(&t)?;
            let snapshot = t.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
            let part_cols = snapshot.metadata().partition_columns().clone();

            // 1.3 Collect Active Files (Convert View -> Add)
            let binding = t.clone();
            let mut stream = binding.get_active_add_actions_by_partitions(&[]);
            let mut files = Vec::new();
            
            while let Some(item) = stream.next().await {
                let view = item.map_err(|e| PolarsError::ComputeError(format!("Stream error: {}", e).into()))?;
                // 使用我们写的 helper 转换
                files.push(view_to_add_action(&view));
            }

            Ok::<_, PolarsError>((t, schema, part_cols, files))
        })?;

        // =================================================================================
        // Step 2: 核心执行循环 (Execution Phase - Sync / Mixed)
        // =================================================================================
        // 这里不使用 block_on 包裹整个循环，以免 execute_cow 内部的 block_on 导致死锁/panic。
        
        // 2.1 Determine Strategy
        // (需要 snapshot，table 已经加载了，直接取)
        let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?;
        let strategy = DeleteStrategy::determine(snapshot, false);

        // 2.2 Build Context
        let ctx = DeleteContext::new(
            &table,
            predicate_expr,
            polars_schema,
            partition_cols,
            strategy,
            cloud_args
        );

        let mut actions_to_commit = Vec::new();

        // 2.3 Loop Files
        for file in all_files {
            // A. Pruning (Partition & Stats)

            let decision = prune_file_from_add(&ctx, &file)?; 

            match decision {
                FileActionDecision::Skip => continue,
                
                FileActionDecision::FullDrop(action) => {
                    actions_to_commit.push(action);
                },
                
                FileActionDecision::Process { file_view } => {
                    // B. Strategy Dispatch
                    let new_actions = match ctx.strategy {
                        DeleteStrategy::CopyOnWrite => {
                            // CoW 执行 (包含 Polars 计算 + IO)
                            execute_copy_on_write(&ctx, &file_view)?
                        },
                        DeleteStrategy::MergeOnRead => {
                            // MoR 占位符
                            execute_merge_on_read(&ctx, &file_view)?
                        }
                    };
                    actions_to_commit.extend(new_actions);
                }
            }
        }

        // =================================================================================
        // Step 3: 提交事务 (Commit Phase - Async)
        // =================================================================================
        if !actions_to_commit.is_empty() {
            rt.block_on(async {
                let operation = DeltaOperation::Delete {
                    predicate: None, // 我们已经物理删除了，这里留空或者是原来的 SQL 字符串
                };
                
                let _ver = CommitBuilder::default()
                    .with_actions(actions_to_commit)
                    .build(
                        Some(table.snapshot().map_err(|e| PolarsError::ComputeError(format!("{}", e).into()))?),
                        table.log_store().clone(),
                        operation
                    ).await.map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
                
                Ok::<(), PolarsError>(())
            })?;
        }

        Ok(())
    })
}

// #[unsafe(no_mangle)]
// pub extern "C" fn pl_io_delta_delete_1(
//     table_path_ptr: *const c_char,
//     predicate_ptr: *mut ExprContext, 
//     cloud_provider: u8,
//     cloud_retries: usize,
//     cloud_retry_timeout_ms: u64,      
//     cloud_retry_init_backoff_ms: u64, 
//     cloud_retry_max_backoff_ms: u64, 
//     cloud_cache_ttl: u64,
//     cloud_keys: *const *const c_char,
//     cloud_values: *const *const c_char,
//     cloud_len: usize
// ) {
//     ffi_try_void!({
//         // 1. Arg Parsing
//         let path_str = ptr_to_str(table_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
//         let table_url = parse_table_url(path_str)?;
//         let delta_storage_options = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        
//         // Take ownership of Expr
//         let predicate_ctx = unsafe { *Box::from_raw(predicate_ptr) };

//         let root_expr = &predicate_ctx.inner;
//         let mut key_bounds = HashMap::new();
//         extract_bounds_from_expr(root_expr, &mut key_bounds);

//         let rt = get_runtime();

//         // 2. Load Table & Get Active Actions (Sync Block)
//         let (table, polars_schema, partition_cols, active_add_actions) = rt.block_on(async {
//             // 2.1 Load Table
//             let t = DeltaTable::try_from_url_with_storage_options(table_url.clone(), delta_storage_options.clone())
//                 .await.map_err(|e| PolarsError::ComputeError(format!("Delta load error: {}", e).into()))?;
//             let scan_t = t.clone();
//             // 2.2 Get Schema & Partition Cols from Snapshot
//             let snapshot = scan_t.snapshot().map_err(|e| PolarsError::ComputeError(format!("Failed to get snapshot: {}", e).into()))?;
//             let schema = get_polars_schema_from_delta(&t)?;
//             let metadata = snapshot.metadata();
//             let part_cols = metadata.partition_columns().clone();

//             // 2.3 Get Actions
//             let mut stream = scan_t.get_active_add_actions_by_partitions(&[]);
//             let mut actions = Vec::new();
            
//             while let Some(item) = stream.next().await {
//                 // Unwrap DeltaResult<LogicalFileView>
//                 let view = item.map_err(|e| PolarsError::ComputeError(format!("Stream error: {}", e).into()))?;
//                 actions.push(view);
//             }

//             Ok::<_, PolarsError>((t, schema, part_cols, actions))
//         })?;

//         // // 3. Get actions

//         let mut actions = Vec::new();
//         let object_store = table.object_store(); 
//         let table_root = table.table_url().to_string(); 

//         for view in active_add_actions {
//             let file_path_str = view.path().clone(); 
            
//             let partition_struct_opt = view.partition_values();
//             let (p_fields, p_values) = match &partition_struct_opt {
//                 Some(sd) => (sd.fields(), sd.values()),
//                 None => (&[][..], &[][..]), 
//             };
//             // =================================================================================
//             // PHASE 1: Partition Pruning (The "Fast Drop" Check)
//             // =================================================================================
//             // Try build a single row DataFrame to predicate
//             let mut can_skip_scan = false;
//             let mut fast_drop = false;

//             if !partition_cols.is_empty() {
//                 // Build Mini-DataFrame from partition values
//                 let mut columns = Vec::with_capacity(partition_cols.len());
//                 for target_col in &partition_cols {
//                     // Find Partition value from actions
//                     let val_str_opt = p_fields.iter()
//                         .position(|f| f.name() == target_col) // Get index
//                         .map(|idx| &p_values[idx])            // Get Scalar
//                         .filter(|scalar| !scalar.is_null())   // Filter Null
//                         .map(|scalar| {
//                              scalar.serialize()
//                         });

//                     let dtype = polars_schema.get_field(target_col).map(|f| f.dtype.clone()).unwrap_or(DataType::String);
                    
//                     // Build Series
//                     let s = match val_str_opt {
//                         Some(v) => Series::new(target_col.into(), &[v]).cast(&dtype)?,
//                         None => Series::new_null(target_col.into(), 1).cast(&dtype)?
//                     };
//                     columns.push(s.into());
//                 }
                
//                 if let Ok(mini_df) = DataFrame::new(1,columns) {
//                     // Try to evaluate predicate
//                     // Clone expr because we might need it later if this fails
//                     let eval_result = mini_df.lazy()
//                         .select([predicate_ctx.inner.clone().alias("result")])
//                         .collect_with_engine(Engine::Streaming);

//                     match eval_result {
//                         Ok(res) => {
//                             // [Success]: Predicate only depends on Partition Columns!
//                             // Check the boolean result
//                             if let Ok(bool_s) = res.column("result") {
//                                 if let Ok(is_match) = bool_s.bool() {
//                                     if is_match.get(0) == Some(true) {
//                                         // Partition Matched Predicate -> Drop entire file
//                                         fast_drop = true;
//                                     } else {
//                                         // Partition Did NOT Match -> Keep entire file (No-op)
//                                         // Do nothing for this file loop
//                                     }
//                                     can_skip_scan = true;
//                                 }
//                             }
//                         },
//                         Err(_e) => {
//                             // [Failure]: Error likely means "ColumnNotFound".
//                             // This confirms the Predicate involves Data Columns (e.g. "id").
//                             // We MUST proceed to scan the file content.
//                             // Ignore error and fall through to Phase 2.
//                         }
//                     }
//                 }
//             }

//             // --- Apply Pruning Result ---
//             if can_skip_scan {
//                 if fast_drop {
//                     // Fast Drop: Mark file for removal, skip IO
//                     let remove = Remove {
//                         path: file_path_str.to_string().clone(),
//                         deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
//                         data_change: true,
//                         ..Default::default()
//                     };
//                     actions.push(Action::Remove(remove));
//                 }
//                 // If fast_drop is false, we simply keep the file (do nothing), effectively skipping it.
//                 continue; 
//             }

//             // =========================================================
//              // Phase 1.5: Stats Pruning (Auto-Derived)
//              // =========================================================
//             let stats_struct = view.stats()
//                     .map(|s| serde_json::from_str(&s).unwrap_or(serde_json::Value::Null));
//              if !key_bounds.is_empty() {

//                  let mut file_overlaps = true;
//                  for (key, (src_min, src_max)) in &key_bounds {
//                      if !check_file_overlap_optimized(&stats_struct, key, src_min, src_max) {
//                          file_overlaps = false;
//                          break;
//                      }
//                  }
                 
//                  if !file_overlaps {
//                      continue;
//                  }
//             }

//             // =================================================================================
//             // PHASE 2: Scan & Filter (Stats Pushdown Optimized)
//             // =================================================================================

//             let total_rows_opt = stats_struct.as_ref()
//                 .and_then(|v| v.get("numRecords"))
//                 .and_then(|v| v.as_i64());
            
//             let full_scan_path = format!("{}/{}", table_root.trim_end_matches('/'), file_path_str);
//             let scan_cloud_options = unsafe {
//                 build_cloud_options(
//                     cloud_provider, cloud_retries, cloud_retry_timeout_ms,
//                     cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
//                     cloud_keys, cloud_values, cloud_len
//                 )
//             };
            
//             let mut lf = LazyFrame::scan_parquet(
//                 PlRefPath::new(full_scan_path.clone()), 
//                 ScanArgsParquet { cloud_options: scan_cloud_options, ..Default::default() }
//             )?;

//             // Inject Partitions
//             for target_col in &partition_cols {
//                 // Find Value
//                 let val_str_opt = p_fields.iter()
//                     .position(|f| f.name() == target_col)
//                     .map(|idx| &p_values[idx])
//                     .filter(|scalar| !scalar.is_null())
//                     .map(|scalar| {
//                          scalar.serialize()
//                     });

//                 // Build Literal Expr
//                 let lit_expr = match val_str_opt {
//                     Some(v) => lit(v),
//                     None => lit(NULL)
//                 };

//                 // Cast and Inject
//                 let final_expr = if let Some(dtype) = polars_schema.get_field(target_col).map(|f| f.dtype.clone()) {
//                     lit_expr.cast(dtype)
//                 } else {
//                     lit_expr
//                 };
                
//                 lf = lf.with_column(final_expr.alias(target_col));
//             }

//             // [Optimization]: Use filter() first to allow Parquet Stats Pushdown
//             // If Parquet Min/Max stats prove "No Match", filter returns empty LF instantly.
//             let has_match_df = lf.clone()
//                 .filter(root_expr.clone())
//                 .limit(1)
//                 .collect_with_engine(Engine::Streaming)?;
            
//             if has_match_df.height() == 0 {
//                 // Case 1: Keep (Predicate missed)
//                 continue;
//             }
            
//             let is_full_drop = if let Some(_total) = total_rows_opt {
//                 let keep_expr = root_expr.clone().not();
//                 let has_keep_df = lf.clone()
//                     .filter(keep_expr)
//                     .limit(1) 
//                     .collect_with_engine(Engine::Streaming)?;
                
//                 has_keep_df.height() == 0
//             } else {
//                 let counts_df = lf.clone()
//                     .select([
//                         len().alias("total"), 
//                         root_expr.clone().cast(DataType::UInt32).sum().alias("matched") 
//                     ])
//                     .collect_with_engine(Engine::Streaming)?;

//                 let total = counts_df.column("total")?.u32()?.get(0).unwrap_or(0) as i64;
//                 let matched = counts_df.column("matched")?.u32()?.get(0).unwrap_or(0) as i64;

//                 // if matched == total => Drop
//                 matched == total
//             };

//             if is_full_drop {
//                 // Case 2: Drop (Full match)
//                 let remove = Remove {
//                     path: file_path_str.to_string().clone(),
//                     deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
//                     data_change: true,
//                     ..Default::default()
//                 };
//                 actions.push(Action::Remove(remove));
//             } else {
//                 // Case 3: Rewrite (Partial match)
//                 let remove = Remove {
//                     path: file_path_str.to_string().clone(),
//                     deletion_timestamp: Some(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64),
//                     data_change: true,
//                     ..Default::default()
//                 };
//                 actions.push(Action::Remove(remove));

//                 // Rewrite: Keep rows that do NOT match
//                 let keep_expr = predicate_ctx.inner.clone().not();
                
//                 // Construct Selector to drop partition cols
//                 let drop_names: Vec<PlSmallStr> = partition_cols.iter()
//                     .map(|s| PlSmallStr::from_string(s.clone()))
//                     .collect();
//                 let drop_selector = Selector::ByName { names: drop_names.into(), strict: true };

//                 let new_lf = lf.filter(keep_expr).drop(drop_selector);

//                 let new_name = format!("part-{}-{}.parquet", Utc::now().timestamp_millis(), Uuid::new_v4());
//                 let parent_dir = std::path::Path::new(file_path_str.as_ref()) 
//                     .parent()
//                     .map(|p| p.to_string_lossy().to_string())
//                     .unwrap_or_else(|| "".to_string());
//                 let relative_new_path = if parent_dir.is_empty() { new_name.clone() } else { format!("{}/{}", parent_dir, new_name) };
//                 let full_write_path = format!("{}/{}", table_root.trim_end_matches('/'), relative_new_path);

//                 let write_opts = build_parquet_write_options(1, -1, true, 0, 0, -1)
//                     .map_err(|e| PolarsError::ComputeError(format!("Options error: {}", e).into()))?;
                
//                 let target = SinkTarget::Path(PlRefPath::new(&full_write_path));
//                 let unified_args = unsafe {
//                     build_unified_sink_args(
//                         false, false, 0, cloud_provider, cloud_retries, cloud_retry_timeout_ms,
//                         cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms, cloud_cache_ttl,
//                         cloud_keys, cloud_values, cloud_len 
//                     )
//                 };

//                 new_lf.sink(SinkDestination::File { target }, FileWriteFormat::Parquet(write_opts), unified_args)?
//                     .collect_with_engine(Engine::Streaming)?;

//                 // Read Stats & Add Action
//                 let (file_size, stats_json) = rt.block_on(async {
//                     let path = Path::from(relative_new_path.clone());
//                     let meta = object_store.head(&path).await
//                          .map_err(|e| PolarsError::ComputeError(format!("Head error: {}", e).into()))?;
//                     let mut reader = ParquetObjectReader::new(object_store.clone(), path.clone())
//                             .with_file_size(meta.size as u64);
//                     let footer = reader.get_metadata(None).await
//                             .map_err(|e| PolarsError::ComputeError(format!("Footer error: {}", e).into()))?;
//                     let (_, json) = extract_delta_stats(&footer)?;
//                     Ok::<_, PolarsError>((meta.size as i64, json))
//                 })?;

//                 let new_partition_values = parse_hive_partitions(&relative_new_path, &partition_cols);
//                 let add = Add {
//                     path: relative_new_path,
//                     size: file_size,
//                     partition_values: new_partition_values,
//                     modification_time: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64,
//                     data_change: true,
//                     stats: Some(stats_json),
//                     ..Default::default()
//                 };
//                 actions.push(Action::Add(add));
//             }
//         }
//         // 4. Commit
//         if !actions.is_empty() {
//             rt.block_on(async {
//                 let operation = DeltaOperation::Delete {
//                     predicate: None, 
//                 };
                
//                 let _ver = CommitBuilder::default()
//                     .with_actions(actions)
//                     .build(
//                         table.snapshot().ok().map(|s| s as &dyn transaction::TableReference),
//                         table.log_store().clone(),
//                         operation
//                     ).await.map_err(|e| PolarsError::ComputeError(format!("Commit failed: {}", e).into()))?;
                
//                 Ok::<(), PolarsError>(())
//             })?;
//         }
//         Ok(())
//     })
// }