use polars::prelude::file_provider::HivePathProvider;
use polars::prelude::*;
use polars_io::cloud::CloudOptions;
use polars_io::{HiveOptions, RowIndex};
use std::ffi::CStr;
use std::os::raw::c_char;
use crate::types::{SchemaContext, SelectorContext};
use crate::utils::{ map_parallel_strategy,  map_sync_on_close,ptr_to_schema_ref, ptr_to_str};

fn ms_to_duration(ms: u64) -> Option<std::time::Duration> {
    if ms == 0 {
        None
    } else {
        Some(std::time::Duration::from_millis(ms))
    }
}

pub(crate) unsafe fn build_cloud_options(
    provider_code: u8,
    retries: usize,
    retry_timeout_ms: u64,      
    retry_init_backoff_ms: u64, 
    retry_max_backoff_ms: u64,  
    cache_ttl: u64,
    keys_ptr: *const *const c_char,
    vals_ptr: *const *const c_char,
    len: usize
) -> Option<CloudOptions> {

    if provider_code == 0 {
        return None;
    }

    let scheme = match provider_code {
        1 => Some(CloudScheme::S3),     // AWS, S3, S3a 
        2 => Some(CloudScheme::Azure),  // Azure, Abfs, Abfss 
        3 => Some(CloudScheme::Gcs),    // Gcs, Gs 
        4 => Some(CloudScheme::Http),   // Http, Https
        5 => Some(CloudScheme::Hf),     // Hugging Face
        _ => {
            eprintln!("Warning: Unknown cloud provider code: {}", provider_code);
            None
        }
    };

    let mut params = Vec::with_capacity(len);
    if !keys_ptr.is_null() && !vals_ptr.is_null() && len > 0 {
        let keys_slice = unsafe {std::slice::from_raw_parts(keys_ptr, len)};
        let vals_slice = unsafe {std::slice::from_raw_parts(vals_ptr, len)};

        for i in 0..len {
            let k_ptr = keys_slice[i];
            let v_ptr = vals_slice[i];
            if !k_ptr.is_null() && !v_ptr.is_null() {
                let k = unsafe {CStr::from_ptr(k_ptr).to_string_lossy().into_owned()};
                let v = unsafe {CStr::from_ptr(v_ptr).to_string_lossy().into_owned()};
                params.push((k, v));
            }
        }
    }

    let mut opts = match CloudOptions::from_untyped_config(scheme, params) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("Cloud config error: {}", e);
            return None;
        }
    };

    opts.retry_config.max_retries = Some(retries);
    opts.retry_config.retry_timeout = ms_to_duration(retry_timeout_ms);
    opts.retry_config.retry_init_backoff = ms_to_duration(retry_init_backoff_ms);
    opts.retry_config.retry_max_backoff = ms_to_duration(retry_max_backoff_ms);

    opts.file_cache_ttl = cache_ttl;

    Some(opts)
}

pub(crate)fn build_scan_args(
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    allow_missing_columns: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    schema_ptr: *mut SchemaContext,        
    hive_schema_ptr: *mut SchemaContext,   
    try_parse_hive_dates: bool,      
    rechunk: bool,
    cache: bool,       
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
) -> ScanArgsParquet {
    let mut args = ScanArgsParquet::default();

    if !n_rows.is_null() { unsafe { args.n_rows = Some(*n_rows); } }
    args.parallel = map_parallel_strategy(parallel_code);
    args.low_memory = low_memory;
    args.use_statistics = use_statistics;
    args.glob = glob;
    args.allow_missing_columns = allow_missing_columns;
    args.rechunk = rechunk; 
    args.cache = cache;     

    if !row_index_name_ptr.is_null() {
            let name = ptr_to_str(row_index_name_ptr).unwrap();
            args.row_index = Some(RowIndex {
                name: PlSmallStr::from_str(name),
                offset: row_index_offset as u32,
            });
    }

    if !include_path_col_ptr.is_null() {
            let col_name = ptr_to_str(include_path_col_ptr).unwrap();
            args.include_file_paths = Some(PlSmallStr::from_str(col_name));
    }

    // --- Schema ---
    args.schema = unsafe { ptr_to_schema_ref(schema_ptr) };

    // --- HiveOptions ---
    let hive_schema = unsafe { ptr_to_schema_ref(hive_schema_ptr) };
    
    let hive_enabled = hive_schema.is_some() || try_parse_hive_dates;
    
    args.hive_options = HiveOptions {
        enabled: Some(hive_enabled), 
        hive_start_idx: 0,
        schema: hive_schema, 
        try_parse_dates: try_parse_hive_dates,
    };

    args.cloud_options = unsafe {
        build_cloud_options(
            cloud_provider, 
            cloud_retries, 
            cloud_retry_timeout_ms,      
            cloud_retry_init_backoff_ms, 
            cloud_retry_max_backoff_ms,  
            cloud_cache_ttl, 
            cloud_keys, 
            cloud_values, 
            cloud_len
        )
    };

    args
}

#[inline]
pub(crate) unsafe fn build_unified_sink_args(
    mkdir: bool,
    maintain_order: bool,
    sync_on_close_code: u8,
    // --- Cloud Options (Flattened) ---
    cloud_provider: u8,
    cloud_retries: usize,
    cloud_retry_timeout_ms: u64,
    cloud_retry_init_backoff_ms: u64,
    cloud_retry_max_backoff_ms: u64,
    cloud_cache_ttl: u64,
    cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char,
    cloud_len: usize
) -> UnifiedSinkArgs {
    
    // CloudOptions
    let cloud_options = unsafe {build_cloud_options(
        cloud_provider,
        cloud_retries,
        cloud_retry_timeout_ms,
        cloud_retry_init_backoff_ms,
        cloud_retry_max_backoff_ms,
        cloud_cache_ttl,
        cloud_keys,
        cloud_values,
        cloud_len
    ).map(Arc::new)};

    // SyncOnClose
    let sync_on_close = map_sync_on_close(sync_on_close_code);

    // Return
    UnifiedSinkArgs {
        mkdir,
        maintain_order,
        sync_on_close,
        cloud_options,
    }
}

pub(crate) unsafe fn build_partitioned_destination(
    base_path_ptr: *const c_char,
    file_extension: &str, // ".parquet", ".ipc", ".csv"
    schema: &Schema,      
    partition_by_ptr: *mut SelectorContext, // nullable
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,
) -> PolarsResult<SinkDestination> {
    
    // Parse base path
    let base_path_str = ptr_to_str(base_path_ptr)
        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

    // Partition Strategy (Keyed vs FileSize)
    let strategy = if !partition_by_ptr.is_null() {
        // A. Keyed Strategy (Hive Style: key=value/...)
        let selector_ctx = unsafe {Box::from_raw(partition_by_ptr)};
        
        let ignored = PlHashSet::new();
        // Use schema to analyze column name
        let names_set = selector_ctx.inner.into_columns(schema, &ignored)?;
        
        // Convert to col("name") expression
        let keys: Vec<Expr> = names_set.iter()
            .map(|name| col(name.clone()))
            .collect();

        if keys.is_empty() {
            PartitionStrategy::FileSize
        } else {
            PartitionStrategy::Keyed {
                keys,
                include_keys,
                keys_pre_grouped,
            }
        }
    } else {
        // FileSize Strategy
        PartitionStrategy::FileSize
    };

    // Build HivePathProvider
    let hive_provider = HivePathProvider {
        extension: PlSmallStr::from_str(file_extension),
    };
    let file_path_provider = Some(file_provider::FileProviderType::Hive(hive_provider));

    // Return SinkDestination
    Ok(SinkDestination::Partitioned {
        base_path: PlRefPath::from(base_path_str),
        file_path_provider,
        partition_strategy: strategy,
        max_rows_per_file: max_rows_per_file as IdxSize,
        approximate_bytes_per_file: approx_bytes_per_file,
    })
}

