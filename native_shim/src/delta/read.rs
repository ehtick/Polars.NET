use deltalake::DeltaTableBuilder;
use futures::StreamExt;
use polars::{error::{PolarsError, PolarsResult}, frame::DataFrame, prelude::{LazyFrame, PlRefPath}};
use polars_buffer::Buffer;
use polars_io::HiveOptions;
use url::Url;
use std::{ffi::c_char, sync::Arc};

use crate::delta::utils::{build_delta_storage_options_map, get_runtime,get_polars_schema_from_delta};
use crate::io::build_scan_args;
use crate::types::{LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;
use crate::delta::deletion_vector::*;

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_delta(
    path_ptr: *const c_char,
    // --- Time Travel Args ---
    version: *const i64,
    datetime_ptr: *const c_char,
    // --- Scan Args ---
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    rechunk: bool, 
    cache: bool,   
    // --- Optional Names ---
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- Schema ---
    schema_ptr: *mut SchemaContext, 
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool,
    // --- Cloud Options ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, 
    cloud_retry_max_backoff_ms: u64, 
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        let version_val = if version.is_null() { None } else { unsafe { Some(*version) } };
        let datetime_str = if datetime_ptr.is_null() { None } else { Some(ptr_to_str(datetime_ptr).unwrap()) };

        let table_url = if let Ok(u) = Url::parse(path_str) { u } else {
            let abs = std::fs::canonicalize(path_str).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            Url::from_directory_path(abs).map_err(|_| PolarsError::ComputeError("Invalid path".into()))?
        };

        let delta_opts = build_delta_storage_options_map(cloud_keys, cloud_values, cloud_len);
        // Load Delta Table & Extract Info (Async)
        let rt = get_runtime();
        let (table, polars_schema, partition_cols,clean_paths, dirty_infos) = rt.block_on(async {
            let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            if let Some(v) = version_val { builder = builder.with_version(v); }
            else if let Some(dt) = datetime_str { builder = builder.with_datestring(dt).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?; }
            
            let table = builder.with_storage_options(delta_opts).load().await.map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

            let final_schema = get_polars_schema_from_delta(&table)?;

            let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
            let partition_cols = snapshot.metadata().partition_columns().clone();

            // let files = table.get_file_uris().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            // let uris: Vec<String> = files.map(|s| s.to_string()).collect();
            // 准备容器
            let mut clean = Vec::new();
            let mut dirty = Vec::new(); // 存储 (绝对路径, DV描述符)

            // 获取迭代器 (返回 LogicalFileView)
            let binding = table.clone();
            let mut stream = binding.get_active_add_actions_by_partitions(&[]);
            let binding_table_url = table.table_url().to_string();
            let table_root = binding_table_url.trim_end_matches('/'); // e.g. "s3://bucket/table"

            while let Some(item) = stream.next().await {
                // item 是 DeltaResult<LogicalFileView>
                let view = item.map_err(|e| PolarsError::ComputeError(format!("Action stream error: {}", e).into()))?;
                
                // 1. 构建绝对路径 (view.path() 返回的是相对路径字符串)
                let full_path = format!("{}/{}", table_root, view.path());

                // 2. 检查是否有 Deletion Vector
                // [关键修改] 使用 deletion_vector_descriptor() 方法
                if let Some(dv_descriptor) = view.deletion_vector_descriptor() {
                    // 有 DV -> 放入脏列表，保存 DV 信息
                    dirty.push((full_path, dv_descriptor));
                } else {
                    // 无 DV -> 放入干净列表
                    clean.push(full_path);
                }
            }
            Ok::<_, PolarsError>((table, final_schema, partition_cols,clean, dirty))
        })?;

        let allow_missing_columns = true;

        // 4. Build Basic Polars Scan Args
        let mut args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, allow_missing_columns, 
            row_index_name_ptr, row_index_offset, include_path_col_ptr,
            schema_ptr, 
            hive_schema_ptr, try_parse_hive_dates,
            rechunk, cache,
            cloud_provider, cloud_retries,    cloud_retry_timeout_ms,      
            cloud_retry_init_backoff_ms, 
            cloud_retry_max_backoff_ms,  cloud_cache_ttl, 
            cloud_keys, cloud_values, cloud_len
        );

        // 5. Inject Delta Schema 
        if args.schema.is_none() {
            args.schema = Some(Arc::new(polars_schema.clone()));
        }

        // 4. [FIXED] Inject Hive Partition Schema 
        // 逻辑修正：使用 with_capacity + insert 构建 Schema
        if hive_schema_ptr.is_null() && !partition_cols.is_empty() {
            // 按照参考代码，创建一个指定容量的空 Schema
            let mut part_schema = polars::prelude::Schema::with_capacity(partition_cols.len());
            
            // 遍历 Delta 记录的分区列名
            for col_name in &partition_cols {
                // 从完整 Schema 中查找该列的类型
                if let Some(dtype) = polars_schema.get(col_name) {
                    // 使用 insert 方法插入列 (key 需要转为 SmartString 或类似，into() 通常能处理)
                    part_schema.insert(col_name.into(), dtype.clone());
                }
            }
            
            // 如果成功提取到了分区列
            if !part_schema.is_empty() {
                // 显式覆盖 HiveOptions
                args.hive_options = HiveOptions {
                    enabled: Some(true),
                    hive_start_idx: 0,
                    schema: Some(Arc::new(part_schema)), // 注入构建好的 schema
                    try_parse_dates: try_parse_hive_dates, // 沿用传入的配置
                };
            }
        }

        // =========================================================
        // Phase 3: 构建 LazyFrames
        // =========================================================
        let mut lfs = Vec::new();

        // 3.1 处理 Clean Files (批量，高性能)
        if !clean_paths.is_empty() {
            let pl_paths: Vec<PlRefPath> = clean_paths.iter().map(|s| PlRefPath::new(s)).collect();
            let buffer: Buffer<PlRefPath> = pl_paths.into();
            
            // 使用 args.clone() 
            let lf_clean = LazyFrame::scan_parquet_files(buffer, args.clone())?;
            lfs.push(lf_clean);
        }

        // 3.2 处理 Dirty Files (逐个处理，应用 DV)
        if !dirty_infos.is_empty() {
            let object_store = table.object_store(); 
            let table_root = deltalake::Path::from(table.table_url().to_string());

            // 既然涉及 IO (读 DV)，这部分在 runtime block_on 里做
            let dirty_lfs = rt.block_on(async {
                let mut processed = Vec::with_capacity(dirty_infos.len());
                
                // 遍历我们刚才收集的 (path, dv) 元组
                for (full_path, dv_descriptor) in dirty_infos {
                    
                    // A. Scan 单文件
                    let mut lf = LazyFrame::scan_parquet(
                        PlRefPath::new(&full_path), 
                        args.clone()
                    )?;

                    // B. 读取并应用 DV
                    // read_deletion_vector 已经在之前实现好了
                    let bitmap = read_deletion_vector(
                        object_store.clone(), 
                        &dv_descriptor, 
                        &table_root
                    ).await?;

                    // apply_deletion_vector 纯 CPU 操作
                    lf = apply_deletion_vector(lf, bitmap)?;
                    
                    processed.push(lf);
                }
                Ok::<_, PolarsError>(processed)
            })?;
            
            lfs.extend(dirty_lfs);
        }

        // // 6. Scan
        // let pl_paths: Vec<PlRefPath> = file_uris.iter().map(|s| PlRefPath::new(s)).collect();
        // let buffer: Buffer<PlRefPath> = pl_paths.into();
        // =========================================================
        // Phase 4: Union / Concat
        // =========================================================
        if lfs.is_empty() {
            let lf = polars::prelude::IntoLazy::lazy(DataFrame::empty_with_schema(&polars_schema));
            return Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })));
        }

        // Concat
        if lfs.len() == 1 {
            // pop() 拿出第一个元素，避免 clone
            let lf = lfs.pop().unwrap(); 
            return Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })));
        }

        // [FIXED] 使用最新的 concat + UnionArgs 逻辑
        let args = polars::prelude::UnionArgs {
            parallel: true,           // 允许并行合并
            rechunk: false,           // Lazy 模式下通常不需要立即 rechunk
            to_supertypes: true,      // 允许类型自动提升 (重要！应对 Schema 演进)
            maintain_order: false,    // Delta 表通常不保证读取顺序，false 更快
            strict: false,            // 允许 schema 不严格一致 (配合 diagonal)
            diagonal: true,           // [关键] 开启对角合并，应对不同文件列数不一致的情况
            from_partitioned_ds: false,
        };

        let final_lf = polars::prelude::concat(lfs, args)
            .map_err(|e| PolarsError::ComputeError(format!("Concat failed: {}", e).into()))?;
        
        // let lf = LazyFrame::scan_parquet_files(buffer, args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: final_lf })))
    })
}