use deltalake::DeltaTableBuilder;
use polars::{error::{PolarsError, PolarsResult}, prelude::{LazyFrame, PlRefPath}};
use polars_buffer::Buffer;
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
        let (file_uris, polars_schema) = rt.block_on(async {
            let mut builder = DeltaTableBuilder::from_url(table_url).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            if let Some(v) = version_val { builder = builder.with_version(v); }
            else if let Some(dt) = datetime_str { builder = builder.with_datestring(dt).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?; }
            
            let table = builder.with_storage_options(delta_opts).load().await.map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

            let final_schema = get_polars_schema_from_delta(&table)?;

            let files = table.get_file_uris().map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
            let uris: Vec<String> = files.map(|s| s.to_string()).collect();
            Ok::<_, PolarsError>((uris, final_schema))
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
            args.schema = Some(Arc::new(polars_schema));
        }
        
        // 6. Scan
        let pl_paths: Vec<PlRefPath> = file_uris.iter().map(|s| PlRefPath::new(s)).collect();
        let buffer: Buffer<PlRefPath> = pl_paths.into();
        
        let lf = LazyFrame::scan_parquet_files(buffer, args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

// pub(crate) fn scan_delta_files(
//     table: &deltalake::DeltaTable,
//     file_paths: Vec<String>, 
//     cloud_options: Option<polars_io::cloud::CloudOptions>, 
// ) -> Result<LazyFrame, PolarsError> {
    
//     if file_paths.is_empty() {
//         let schema = get_polars_schema_from_delta(table)?;
//         return Ok(polars::prelude::IntoLazy::lazy(polars::frame::DataFrame::empty_with_schema(&schema)));
//     }

//     let table_uri = table.table_url();
    
//     let full_paths: Vec<String> = file_paths.iter().map(|p| {
//         if p.starts_with("s3://") || p.starts_with("abfss://") || p.starts_with("gs://") || p.starts_with("/") {
//             p.to_string() // 直接要 String
//         } else {
//             let base = table_uri.to_string().trim_end_matches('/').to_string();
//             let relative = p.trim_start_matches('/');
//             format!("{}/{}", base, relative) // format! 返回的就是 String，直接用
//         }
//     }).collect();

//     // 2. Schema 注入 (Crucial!)
//     let delta_polars_schema = get_polars_schema_from_delta(table)?;

//     // 3. 构建 ScanArgsParquet (Manual Construction)
//     let args = polars::prelude::ScanArgsParquet {
//         n_rows: None,          // 读全量
//         cache: false,          // Optimize 是一次性操作，不需要缓存
//         parallel: polars::prelude::ParallelStrategy::Auto,
//         rechunk: false,        // 流式处理不需要 rechunk
//         row_index: None,       // 不需要行号
//         low_memory: true,      // 开启低内存模式，防止 OOM
//         cloud_options: cloud_options, // 关键：透传 S3 凭证
//         use_statistics: true,  // 利用 Parquet 统计信息加速读取
//         schema: Some(Arc::new(delta_polars_schema)), // 强校验 Schema
//         ..Default::default()
//     };

//     let pl_paths: Vec<PlRefPath> = full_paths.iter().map(|s| PlRefPath::new(s)).collect();
//     let buffer: Buffer<PlRefPath> = pl_paths.into();
    
//     let lf = LazyFrame::scan_parquet_files(buffer, args)?;

//     Ok(lf)
// }