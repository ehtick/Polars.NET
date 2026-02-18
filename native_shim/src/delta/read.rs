use deltalake::DeltaTableBuilder;
use polars::{error::{PolarsError, PolarsResult}, prelude::{LazyFrame, PlRefPath}};
use polars_buffer::Buffer;
use polars_io::HiveOptions;
use url::Url;
use std::{ffi::c_char, sync::Arc};

use crate::delta::utils::{build_delta_storage_options_map, get_runtime,get_polars_schema_from_delta};
use crate::io::build_scan_args;
use crate::types::{LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_str;

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
        let (file_uris, polars_schema, partition_cols) = rt.block_on(async {
            let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            if let Some(v) = version_val { builder = builder.with_version(v); }
            else if let Some(dt) = datetime_str { builder = builder.with_datestring(dt).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?; }
            
            let table = builder.with_storage_options(delta_opts).load().await.map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

            let final_schema = get_polars_schema_from_delta(&table)?;

            let snapshot = table.snapshot().map_err(|e| PolarsError::ComputeError(format!("Snapshot error: {}", e).into()))?;
            let partition_cols = snapshot.metadata().partition_columns().clone();

            let files = table.get_file_uris().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            let uris: Vec<String> = files.map(|s| s.to_string()).collect();
            Ok::<_, PolarsError>((uris, final_schema, partition_cols))
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
                    try_parse_dates: false, // 沿用传入的配置
                };
            }
        }
        // 6. Scan
        let pl_paths: Vec<PlRefPath> = file_uris.iter().map(|s| PlRefPath::new(s)).collect();
        let buffer: Buffer<PlRefPath> = pl_paths.into();
        
        let lf = LazyFrame::scan_parquet_files(buffer, args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}