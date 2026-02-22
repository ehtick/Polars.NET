use polars::prelude::*;
use polars_buffer::Buffer;
use polars_io::cloud::CloudOptions;
use polars_io::RowIndex;
use std::ffi::CStr;
use std::os::raw::c_char;
use crate::pl_io::csv::csv_utils::map_csv_encoding;
use crate::pl_io::io_utils::build_cloud_options;
use crate::types::{LazyFrameContext, SchemaContext};
use crate::utils::ptr_to_vec_string;

// ==========================================
// CSV Scan (Unified Lazy Pathway)
// ==========================================

#[inline]
unsafe fn apply_scan_csv_options(
    mut reader: LazyCsvReader,
    // --- 1. Core Configs ---
    has_header: bool,
    separator: u8,
    quote_char: u8,           
    eol_char: u8,             
    ignore_errors: bool,
    try_parse_dates: bool,
    low_memory: bool,
    cache: bool,
    glob: bool,
    rechunk: bool,
    raise_if_empty: bool,

    // --- 2. Sizes & Threading ---
    skip_rows: usize,
    skip_rows_after_header: usize,
    skip_lines: usize,
    n_rows_ptr: *const usize,
    infer_schema_len_ptr: *const usize,
    n_threads_ptr: *const usize,
    chunk_size: usize,

    // --- 3. Row Index & Path ---
    row_index_name: *const c_char,
    row_index_offset: usize,
    include_file_paths: *const c_char,

    // --- 4. Schema & Encoding ---
    schema_ptr: *mut SchemaContext,
    encoding: u8, 
    
    // --- 5. Advanced Parsing ---
    null_values_ptr: *const *const c_char, 
    null_values_len: usize,                
    missing_is_null: bool,                 
    comment_prefix: *const c_char,         
    decimal_comma: bool,
    truncate_ragged_lines: bool,
    
    // --- 6. Cloud ---
    cloud_options: Option<CloudOptions>
) -> LazyCsvReader {
    
    // Core
    reader = reader
        .with_has_header(has_header)
        .with_separator(separator)
        .with_quote_char(if quote_char == 0 { None } else { Some(quote_char) }) 
        .with_eol_char(eol_char)                                        
        .with_ignore_errors(ignore_errors)
        .with_try_parse_dates(try_parse_dates)
        .with_low_memory(low_memory)
        .with_cache(cache)
        .with_glob(glob)
        .with_rechunk(rechunk)
        .with_raise_if_empty(raise_if_empty)
        .with_skip_rows(skip_rows)
        .with_skip_rows_after_header(skip_rows_after_header)
        .with_skip_lines(skip_lines)
        .with_missing_is_null(missing_is_null)
        .with_decimal_comma(decimal_comma)
        .with_truncate_ragged_lines(truncate_ragged_lines)
        .with_encoding(map_csv_encoding(encoding));

    if chunk_size != 0 {
        reader = reader.with_chunk_size(chunk_size);
    }

    if !n_rows_ptr.is_null() {
        reader = reader.with_n_rows(Some(unsafe { *n_rows_ptr }));
    }
    
    if !infer_schema_len_ptr.is_null() {
        reader = reader.with_infer_schema_length(Some(unsafe { *infer_schema_len_ptr }));
    }

    if !n_threads_ptr.is_null() {
        reader = reader.with_n_threads(Some(unsafe { *n_threads_ptr }));
    }

    if !schema_ptr.is_null() {
        let schema = unsafe {(*schema_ptr ).schema.clone()};
        reader = reader.with_dtype_overwrite(Some(schema));
    }

    if !row_index_name.is_null() {
        let name_cow = unsafe { CStr::from_ptr(row_index_name).to_string_lossy() };
        reader = reader.with_row_index(Some(RowIndex {
            name: PlSmallStr::from_str(name_cow.as_ref()),
            offset: row_index_offset as u32,
        }));
    }

    if !include_file_paths.is_null() {
        let path_col = unsafe { CStr::from_ptr(include_file_paths).to_string_lossy() };
        reader = reader.with_include_file_paths(Some(PlSmallStr::from_str(path_col.as_ref())));
    }

    if !null_values_ptr.is_null() && null_values_len > 0 {
        let vec_str = unsafe { ptr_to_vec_string(null_values_ptr, null_values_len) };
        if !vec_str.is_empty() {
            let small_strs: Vec<PlSmallStr> = vec_str.into_iter()
                .map(|s| PlSmallStr::from_str(&s))
                .collect();
            reader = reader.with_null_values(Some(NullValues::AllColumns(small_strs)));
        }
    }

    if !comment_prefix.is_null() {
        let s = unsafe { CStr::from_ptr(comment_prefix).to_string_lossy() };
        reader = reader.with_comment_prefix(Some(PlSmallStr::from_str(s.as_ref())));
    }

    if let Some(opts) = cloud_options {
        reader = reader.with_cloud_options(Some(opts));
    }

    reader
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv(
    path: *const c_char,
    
    // --- 1. Core Configs ---
    has_header: bool, separator: u8, quote_char: u8, eol_char: u8,             
    ignore_errors: bool, try_parse_dates: bool, low_memory: bool,
    cache: bool, glob: bool, rechunk: bool, raise_if_empty: bool,

    // --- 2. Sizes & Threads ---
    skip_rows: usize, skip_rows_after_header: usize, skip_lines: usize,
    n_rows_ptr: *const usize, infer_schema_len_ptr: *const usize, 
    n_threads_ptr: *const usize, chunk_size: usize,

    // --- 3. Row Index & Path ---
    row_index_name: *const c_char, row_index_offset: usize, include_file_paths: *const c_char,

    // --- 4. Schema & Encoding ---
    schema_ptr: *mut SchemaContext, encoding: u8, 
    
    // --- 5. Advanced Parsing ---
    null_values_ptr: *const *const c_char, null_values_len: usize,                
    missing_is_null: bool, comment_prefix: *const c_char, decimal_comma: bool, truncate_ragged_lines: bool,
    
    // --- 6. Cloud Params ---
    cloud_provider: u8, cloud_retries: usize, cloud_retry_timeout_ms: u64,      
    cloud_retry_init_backoff_ms: u64, cloud_retry_max_backoff_ms: u64,  
    cloud_cache_ttl: u64, cloud_keys: *const *const c_char,
    cloud_values: *const *const c_char, cloud_len: usize

) -> *mut LazyFrameContext {
    ffi_try!({
        let p_cow = unsafe { CStr::from_ptr(path).to_string_lossy() };
        let p_ref = p_cow.as_ref();

        let cloud_opts = unsafe {
            build_cloud_options(
                cloud_provider, cloud_retries, cloud_retry_timeout_ms,
                cloud_retry_init_backoff_ms, cloud_retry_max_backoff_ms,
                cloud_cache_ttl, cloud_keys, cloud_values, cloud_len
            )
        };

        let mut reader = LazyCsvReader::new(PlRefPath::new(p_ref as &str));

        reader = unsafe {
            apply_scan_csv_options(
                reader, has_header, separator, quote_char, eol_char,
                ignore_errors, try_parse_dates, low_memory, cache, glob, rechunk, raise_if_empty,
                skip_rows, skip_rows_after_header, skip_lines,
                n_rows_ptr, infer_schema_len_ptr, n_threads_ptr, chunk_size,
                row_index_name, row_index_offset, include_file_paths,
                schema_ptr, encoding,
                null_values_ptr, null_values_len, missing_is_null,
                comment_prefix, decimal_comma, truncate_ragged_lines,
                cloud_opts
            )
        };

        let inner = reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv_mem(
    // --- Memory Buffer Input ---
    buffer_ptr: *const u8,
    buffer_len: usize,

    // --- 1. Core Configs ---
    has_header: bool, separator: u8, quote_char: u8, eol_char: u8,             
    ignore_errors: bool, try_parse_dates: bool, low_memory: bool,
    cache: bool, glob: bool, rechunk: bool, raise_if_empty: bool, 

    // --- 2. Sizes & Threads ---
    skip_rows: usize, skip_rows_after_header: usize, skip_lines: usize,
    n_rows_ptr: *const usize, infer_schema_len_ptr: *const usize, 
    n_threads_ptr: *const usize, chunk_size: usize,

    // --- 3. Row Index & Path ---
    row_index_name: *const c_char, row_index_offset: usize, include_file_paths: *const c_char,

    // --- 4. Schema & Encoding ---
    schema_ptr: *mut SchemaContext, encoding: u8, 
    
    // --- 5. Advanced Parsing ---
    null_values_ptr: *const *const c_char, null_values_len: usize,                
    missing_is_null: bool, comment_prefix: *const c_char, decimal_comma: bool, truncate_ragged_lines: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        // Prepare Buffer Source
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let buffer = Buffer::from(slice.to_vec()); 
        let sources = ScanSources::Buffers(Arc::new([buffer]));

        let mut reader = LazyCsvReader::new_with_sources(sources);

        reader = unsafe {
            apply_scan_csv_options(
                reader, has_header, separator, quote_char, eol_char,
                ignore_errors, try_parse_dates, low_memory, cache, glob, rechunk, raise_if_empty,
                skip_rows, skip_rows_after_header, skip_lines,
                n_rows_ptr, infer_schema_len_ptr, n_threads_ptr, chunk_size,
                row_index_name, row_index_offset, include_file_paths,
                schema_ptr, encoding,
                null_values_ptr, null_values_len, missing_is_null,
                comment_prefix, decimal_comma, truncate_ragged_lines,
                None // Memory buffer does not need Cloud Options
            )
        };

        let inner = reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}