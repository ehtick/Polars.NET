use polars::prelude::*;
use polars_buffer::Buffer;
use std::io::Cursor;
use std::os::raw::c_char;
use std::fs::File;
use crate::pl_io::io_utils::build_scan_args;
use crate::types::{DataFrameContext, LazyFrameContext, SchemaContext};
use crate::utils::{map_parallel_strategy,  ptr_to_str};

// ==========================================
// Read Parquet
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_parquet(
    path_ptr: *const c_char,
    columns_ptr: *const *const c_char, columns_len: usize,
    limit: *const usize,              
    parallel_code: u8,
    low_memory: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32
) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::open(path)
            .map_err(|e| PolarsError::ComputeError(format!("File not found: {}", e).into()))?;

        let mut reader = ParquetReader::new(file);

        // --- Columns ---
        if columns_len > 0 {
            let cols = unsafe {
                let mut v = Vec::with_capacity(columns_len);
                for &p in std::slice::from_raw_parts(columns_ptr, columns_len) {
                    if !p.is_null() {
                         v.push(ptr_to_str(p).unwrap().to_string());
                    }
                }
                v
            };
            reader = reader.with_columns(Some(cols));
        }

        // --- Limit (Mapped to Slice) ---
        if !limit.is_null() {
            let n = unsafe { *limit };
            // Offset = 0, Length = n
            reader = reader.with_slice(Some((0, n)));
        }

        // --- Other Options ---
        reader = reader.read_parallel(map_parallel_strategy(parallel_code));
        reader = reader.set_low_memory(low_memory);

        if !row_index_name_ptr.is_null() {
            let name = ptr_to_str(row_index_name_ptr).unwrap();
            let row_index = polars::io::RowIndex {
                name: PlSmallStr::from_str(name),
                offset: row_index_offset as u32,
            };
            reader = reader.with_row_index(Some(row_index));
        }

        let df = reader.finish()?;
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// ---------------------------------------------------------
// Read Parquet (Memory / Bytes)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_parquet_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    columns_ptr: *const *const c_char, columns_len: usize,
    limit: *const usize,
    parallel_code: u8,
    low_memory: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32
) -> *mut DataFrameContext {
    ffi_try!({
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let cursor = Cursor::new(slice);

        let mut reader = ParquetReader::new(cursor);

        // --- Columns ---
        if columns_len > 0 {
            let cols = unsafe {
                let mut v = Vec::with_capacity(columns_len);
                for &p in std::slice::from_raw_parts(columns_ptr, columns_len) {
                    if !p.is_null() { v.push(ptr_to_str(p).unwrap().to_string()); }
                }
                v
            };
            reader = reader.with_columns(Some(cols));
        }

        // --- Limit (Mapped to Slice) ---
        if !limit.is_null() {
            let n = unsafe { *limit };
            reader = reader.with_slice(Some((0, n)));
        }

        // --- Other Options ---
        reader = reader.read_parallel(map_parallel_strategy(parallel_code));
        reader = reader.set_low_memory(low_memory);

        if !row_index_name_ptr.is_null() {
            let name = ptr_to_str(row_index_name_ptr).unwrap();
            let row_index = polars::io::RowIndex {
                name: PlSmallStr::from_str(name),
                offset: row_index_offset as u32,
            };
            reader = reader.with_row_index(Some(row_index));
        }

        let df = reader.finish()?;
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet(
    path_ptr: *const c_char,
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    allow_missing_columns: bool,
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
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, allow_missing_columns, 
            row_index_name_ptr, row_index_offset, include_path_col_ptr,
            schema_ptr, hive_schema_ptr, try_parse_hive_dates,
            rechunk, cache,
            cloud_provider, cloud_retries,cloud_retry_timeout_ms,      
            cloud_retry_init_backoff_ms, 
            cloud_retry_max_backoff_ms,cloud_cache_ttl, 
            cloud_keys, cloud_values, cloud_len
        );

        let lf = LazyFrame::scan_parquet(PlRefPath::new(path), args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner:lf })))
    })
}

// ---------------------------------------------------------
// Scan Parquet (Memory / Buffers)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_parquet_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    n_rows: *const usize,
    parallel_code: u8,
    low_memory: bool,
    use_statistics: bool,
    glob: bool,
    allow_missing_columns: bool,
    rechunk: bool, 
    cache: bool,   
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    schema_ptr: *mut SchemaContext,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool
) -> *mut LazyFrameContext {
    ffi_try!({

        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let buffer = Buffer::from(slice.to_vec()); 
        let sources = ScanSources::Buffers(Arc::new([buffer]));


        let args = build_scan_args(
            n_rows, 
            parallel_code, 
            low_memory, 
            use_statistics, 
            glob, 
            allow_missing_columns, 
            row_index_name_ptr, 
            row_index_offset, 
            include_path_col_ptr,
            schema_ptr, 
            hive_schema_ptr, 
            try_parse_hive_dates,
            rechunk,  
            cache,    
            // --- Cloud Options  ---
            0,                // provider_code = None
            0,                // retries
            0,
            0,
            0,
            0,                // cache_ttl
            std::ptr::null(), // keys_ptr
            std::ptr::null(), // vals_ptr
            0                 // len
        );

        let lf = LazyFrame::scan_parquet_sources(sources, args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner:lf })))
    })
}
