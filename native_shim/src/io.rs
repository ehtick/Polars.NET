use polars::prelude::file_provider::HivePathProvider;
use polars::prelude::*;
use polars_buffer::Buffer;
use polars_io::cloud::CloudOptions;
use polars_io::mmap::MmapBytesReader;
use polars_io::{HiveOptions, RowIndex};
use polars_arrow::ffi::{self, ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::{StructArray};
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use polars_utils::pl_str::PlRefStr;
use std::ffi::{CStr, c_void};
use std::io::{BufReader,Cursor};
use std::os::raw::c_char;
use std::fs::File;
use std::num::NonZeroUsize;
use crate::types::{DataFrameContext, LazyFrameContext, SchemaContext, SelectorContext};
use crate::utils::{map_csv_encoding, map_external_compression, map_json_format, map_parallel_strategy, map_quote_style, map_sync_on_close, ptr_to_opt_pl_str, ptr_to_pl_str, ptr_to_schema_ref, ptr_to_str, ptr_to_vec_string};
use std::path::PathBuf;
use polars_utils::slice_enum::Slice;
use polars_utils::compression::{BrotliLevel, GzipLevel, ZstdLevel};

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

// ==========================================
// Read csv
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_read_csv(
    path: *const c_char,
    
    // --- 1. Columns Projection ---
    col_names_ptr: *const *const c_char,
    col_names_len: usize,

    // --- 2. Core Configs ---
    has_header: bool,
    separator: u8,
    quote_char: u8,           
    eol_char: u8,            
    ignore_errors: bool,
    try_parse_dates: bool,
    low_memory: bool,
    
    // --- 3. Optional Sizes ---
    skip_rows: usize,
    n_rows_ptr: *const usize,
    infer_schema_len_ptr: *const usize,

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
    
    // --- 6. Row Index ---
    row_index_name: *const c_char,         
    row_index_offset: usize,               
) -> *mut DataFrameContext {
    ffi_try!({
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        // --- Encoding ---
        let csv_encoding = map_csv_encoding(encoding);

        // --- Columns ---
        let columns = if col_names_ptr.is_null() || col_names_len == 0 {
            None
        } else {
            let mut cols = Vec::with_capacity(col_names_len);
            for i in 0..col_names_len {
                let c_str = unsafe { *col_names_ptr.add(i) };
                let s = unsafe { CStr::from_ptr(c_str).to_string_lossy() };
                cols.push(PlSmallStr::from_str(&s));
            }
            Some(Arc::from(cols.into_boxed_slice()))
        };

        // --- Basic Options ---
        let n_rows = if n_rows_ptr.is_null() { None } else { Some(unsafe { *n_rows_ptr }) };
        let infer_schema_length = if infer_schema_len_ptr.is_null() { 
            None 
        } else { 
            Some(unsafe { *infer_schema_len_ptr }) 
        };

        let schema_overwrite = if schema_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*schema_ptr }.schema.clone())
        };

        // --- Process Null Values ---
        let null_vals = if !null_values_ptr.is_null() && null_values_len > 0 {
            let vec_str = unsafe { ptr_to_vec_string(null_values_ptr, null_values_len) };
            if vec_str.is_empty() {
                None
            } else {
                let small_strs: Vec<PlSmallStr> = vec_str.into_iter()
                    .map(|s| PlSmallStr::from_str(&s))
                    .collect();
                Some(NullValues::AllColumns(small_strs))
            }
        } else {
            None
        };

        // --- Process Comment Prefix ---
        let comment_prefix_str = if comment_prefix.is_null() {
            None
        } else {
            Some(unsafe { CStr::from_ptr(comment_prefix).to_string_lossy().into_owned() })
        };
        let comment = comment_prefix_str.map(|s| CommentPrefix::new_from_str(&s));

        // --- Process Row Index ---
        let row_index = if !row_index_name.is_null() {
            let name = unsafe { CStr::from_ptr(row_index_name).to_string_lossy() };
            Some(RowIndex {
                offset: row_index_offset as u32, // Polars uses IdxSize (u32) usually
                name: PlSmallStr::from_str(&name),
            })
        } else {
            None
        };

        // --- Construct ParseOptions ---
        let parse_options = CsvParseOptions {
            separator,
            quote_char: if quote_char == 0 { None } else { Some(quote_char) }, 
            eol_char,
            try_parse_dates,
            encoding: csv_encoding,
            null_values: null_vals,
            missing_is_null,
            truncate_ragged_lines: truncate_ragged_lines, 
            comment_prefix: comment,
            decimal_comma,
            ..Default::default()
        };

        // --- Construct ReadOptions ---
        let read_options = CsvReadOptions {
            has_header,
            skip_rows,
            n_rows,
            infer_schema_length,
            ignore_errors,
            low_memory,
            columns, 
            schema_overwrite,
            row_index,
            parse_options: Arc::new(parse_options),
            ..Default::default() 
        };

        // --- Execute ---
        let df = read_options
            .try_into_reader_with_file_path(Some(p.into_owned().into()))?
            .finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv(
    path: *const c_char,
    
    // --- 1. Core Configs ---
    has_header: bool,
    separator: u8,
    quote_char: u8,           
    eol_char: u8,             
    ignore_errors: bool,
    try_parse_dates: bool,
    low_memory: bool,
    cache: bool,
    rechunk: bool,

    // --- 2. Sizes ---
    skip_rows: usize,
    n_rows_ptr: *const usize,
    infer_schema_len_ptr: *const usize,

    // --- 3. Row Index ---
    row_index_name: *const c_char,
    row_index_offset: usize,

    // --- 4. Schema & Encoding ---
    schema_ptr: *mut SchemaContext,
    encoding: u8, 
    
    // --- 5. Advanced Parsing (Sync with Eager) ---
    null_values_ptr: *const *const c_char, 
    null_values_len: usize,                
    missing_is_null: bool,                 
    comment_prefix: *const c_char,         
    decimal_comma: bool,
    chunk_size:usize                   
) -> *mut LazyFrameContext {
    ffi_try!({
        // Path
        let p_cow = unsafe { CStr::from_ptr(path).to_string_lossy() };
        let p_ref = p_cow.as_ref();

        // Encoding
        let csv_encoding = crate::utils::map_csv_encoding(encoding);

        // Option ptr handle
        let n_rows = if n_rows_ptr.is_null() { None } else { Some(unsafe { *n_rows_ptr }) };
        let infer_schema_length = if infer_schema_len_ptr.is_null() { 
            None 
        } else { 
            Some(unsafe { *infer_schema_len_ptr }) 
        };
        
        // Schema overwrite
        let schema_overwrite = if schema_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*schema_ptr }.schema.clone())
        };

        // Row Index
        let row_index = if row_index_name.is_null() {
            None
        } else {
            let name_cow = unsafe { CStr::from_ptr(row_index_name).to_string_lossy() };
            Some(RowIndex {
                name: PlSmallStr::from_str(name_cow.as_ref()),
                offset: row_index_offset as u32,
            })
        };

        // --- Process Null Values ---
        let null_vals = if !null_values_ptr.is_null() && null_values_len > 0 {
            let vec_str = unsafe { ptr_to_vec_string(null_values_ptr, null_values_len) };
            if vec_str.is_empty() {
                None
            } else {
                let small_strs: Vec<PlSmallStr> = vec_str.into_iter()
                    .map(|s| PlSmallStr::from_str(&s))
                    .collect();
                Some(NullValues::AllColumns(small_strs))
            }
        } else {
            None
        };

        // --- Process Comment Prefix (Lazy Version) ---
        let comment = if comment_prefix.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(comment_prefix).to_string_lossy() };
            Some(PlSmallStr::from_str(&s))
        };

        // Build Reader
        let mut reader = LazyCsvReader::new(PlRefPath::new(p_ref as &str))
            .with_has_header(has_header)
            .with_separator(separator)
            .with_quote_char(if quote_char == 0 { None } else { Some(quote_char) }) 
            .with_eol_char(eol_char)                                        
            .with_ignore_errors(ignore_errors)
            .with_try_parse_dates(try_parse_dates)
            .with_low_memory(low_memory)
            .with_cache(cache)
            .with_rechunk(rechunk)
            .with_skip_rows(skip_rows)
            .with_n_rows(n_rows)
            .with_infer_schema_length(infer_schema_length)
            .with_encoding(csv_encoding)
            .with_dtype_overwrite(schema_overwrite) 
            .with_row_index(row_index)
            .with_null_values(null_vals)         
            .with_missing_is_null(missing_is_null) 
            .with_comment_prefix(comment)        
            .with_decimal_comma(decimal_comma);

        if chunk_size != 0 {
                    reader = reader.with_chunk_size(chunk_size);
                }

        // Finish
        let inner = reader.finish()?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv_mem(
    // --- Memory Buffer Input ---
    buffer_ptr: *const u8,
    buffer_len: usize,

    // --- Core Configs ---
    has_header: bool,
    separator: u8,
    quote_char: u8,           
    eol_char: u8,             
    ignore_errors: bool,
    try_parse_dates: bool,
    low_memory: bool,
    cache: bool,
    rechunk: bool,

    // --- Sizes ---
    skip_rows: usize,
    n_rows_ptr: *const usize,
    infer_schema_len_ptr: *const usize,

    // --- Row Index ---
    row_index_name: *const c_char,
    row_index_offset: usize,

    // --- Schema & Encoding ---
    schema_ptr: *mut SchemaContext,
    encoding: u8, 
    
    // --- Advanced Parsing (Sync with File Scan) ---
    null_values_ptr: *const *const c_char, 
    null_values_len: usize,                
    missing_is_null: bool,                 
    comment_prefix: *const c_char,         
    decimal_comma: bool, 
    chunk_size:usize                  
) -> *mut LazyFrameContext {
    ffi_try!({
        // --- 1. Prepare Buffer Source ---
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let buffer = Buffer::from(slice.to_vec()); 
        let sources = ScanSources::Buffers(Arc::new([buffer]));

        // --- 2. Prepare Options ---
        let csv_encoding = crate::utils::map_csv_encoding(encoding);

        let n_rows = if n_rows_ptr.is_null() { None } else { Some(unsafe { *n_rows_ptr }) };
        let infer_schema_length = if infer_schema_len_ptr.is_null() { 
            None 
        } else { 
            Some(unsafe { *infer_schema_len_ptr }) 
        };
        
        let schema_overwrite = if schema_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*schema_ptr }.schema.clone())
        };

        // Row Index
        let row_index = if row_index_name.is_null() {
            None
        } else {
            let name_cow = unsafe { CStr::from_ptr(row_index_name).to_string_lossy() };
            Some(RowIndex {
                name: PlSmallStr::from_str(name_cow.as_ref()),
                offset: row_index_offset as u32,
            })
        };

        // Null Values
        let null_vals = if !null_values_ptr.is_null() && null_values_len > 0 {
            let vec_str = unsafe { ptr_to_vec_string(null_values_ptr, null_values_len) };
            if vec_str.is_empty() {
                None
            } else {
                let small_strs: Vec<PlSmallStr> = vec_str.into_iter()
                    .map(|s| PlSmallStr::from_str(&s))
                    .collect();
                Some(NullValues::AllColumns(small_strs))
            }
        } else {
            None
        };

        // Comment Prefix 
        let comment = if comment_prefix.is_null() {
            None
        } else {
            let s = unsafe { CStr::from_ptr(comment_prefix).to_string_lossy() };
            Some(PlSmallStr::from_str(&s))
        };

        // --- 3. Build Reader ---
        let mut reader = LazyCsvReader::new_with_sources(sources)
            .with_has_header(has_header)
            .with_separator(separator)
            .with_quote_char(if quote_char == 0 { None } else { Some(quote_char) })
            .with_eol_char(eol_char)
            .with_ignore_errors(ignore_errors)
            .with_try_parse_dates(try_parse_dates)
            .with_low_memory(low_memory)
            .with_cache(cache)
            .with_rechunk(rechunk)
            .with_skip_rows(skip_rows)
            .with_n_rows(n_rows)
            .with_infer_schema_length(infer_schema_length)
            .with_encoding(csv_encoding)
            .with_dtype_overwrite(schema_overwrite) 
            .with_row_index(row_index)
            .with_null_values(null_vals)         
            .with_missing_is_null(missing_is_null) 
            .with_comment_prefix(comment)        
            .with_decimal_comma(decimal_comma); 
         
        if chunk_size != 0 {
                    reader = reader.with_chunk_size(chunk_size);
                }
        // --- 4. Finish ---
        let inner = reader.finish()?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}

// ==========================================
// Write & Sink CSV
// ==========================================

unsafe fn build_serialize_options(
    date_format_ptr: *const c_char,
    time_format_ptr: *const c_char,
    datetime_format_ptr: *const c_char,
    float_scientific: i32, // -1: None, 0: False, 1: True
    float_precision: i32,  // -1: None, >=0: Precision
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value_ptr: *const c_char,
    line_terminator_ptr: *const c_char,
    quote_style_code: u8,
) -> SerializeOptions {
    
    // Float Scientific (Option<bool>)
    let float_scientific = match float_scientific {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    };

    // Float Precision (Option<usize>)
    let float_precision = if float_precision >= 0 {
        Some(float_precision as usize)
    } else {
        None
    };

    unsafe {
    SerializeOptions {
        date_format: ptr_to_opt_pl_str(date_format_ptr),
        time_format: ptr_to_opt_pl_str(time_format_ptr),
        datetime_format: ptr_to_opt_pl_str(datetime_format_ptr),
        float_scientific,
        float_precision,
        decimal_comma,
        separator,
        quote_char,
        null: ptr_to_pl_str(null_value_ptr, ""), // 默认为空字符串
        line_terminator: ptr_to_pl_str(line_terminator_ptr, "\n"), // 默认为 \n
        quote_style: map_quote_style(quote_style_code),
    }}
}

pub(crate) unsafe fn build_csv_writer_options(
    // --- CsvWriterOptions Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    check_extension: bool,
    // --- ExternalCompression Params ---
    compression_code: u8,
    compression_level: i32,
    // --- SerializeOptions Params ---
    date_format: *const c_char,
    time_format: *const c_char,
    datetime_format: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value: *const c_char,
    line_terminator: *const c_char,
    quote_style: u8,
) -> CsvWriterOptions {
    
    // SerializeOptions
    let serialize_options = unsafe { build_serialize_options(
        date_format, time_format, datetime_format,
        float_scientific, float_precision, decimal_comma,
        separator, quote_char, null_value, line_terminator, quote_style
    )};

    // Compression
    let compression = map_external_compression(compression_code, compression_level);

    // Batch Size
    let batch_size = NonZeroUsize::new(batch_size).unwrap_or(NonZeroUsize::new(1024).unwrap());

    CsvWriterOptions {
        include_bom,
        compression,
        check_extension, 
        include_header,
        batch_size,
        serialize_options: Arc::new(serialize_options), 
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_csv(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    // --- CSV Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    _compression_code: u8, 
    _compression_level: i32,
    // SerializeOptions
    date_format_ptr: *const c_char,
    time_format_ptr: *const c_char,
    datetime_format_ptr: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value_ptr: *const c_char,
    line_terminator_ptr: *const c_char,
    quote_style: u8,
    // Ignored Unified/Cloud
    _maintain_order: bool, _sync_on_close: u8, _mkdir: bool,
    _cloud_provider: u8, _cloud_retries: usize, _cloud_retry_timeout_ms: u64,
    _cloud_retry_init_backoff_ms: u64, _cloud_retry_max_backoff_ms: u64,
    _cloud_cache_ttl: u64, _cloud_keys: *const *const c_char,
    _cloud_values: *const *const c_char, _cloud_len: usize
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let file = std::fs::File::create(path_str)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create file: {}", e).into()))?;

        // 1. 构建 SerializeOptions (复用)
        let serialize_options = unsafe {
             build_serialize_options(
                date_format_ptr, time_format_ptr, datetime_format_ptr,
                float_scientific, float_precision, decimal_comma,
                separator, quote_char, null_value_ptr, line_terminator_ptr, quote_style
            )
        };

        // 2. 配置 CsvWriter
        let mut writer = CsvWriter::new(file)
            .include_header(include_header)
            .include_bom(include_bom)
            .with_separator(serialize_options.separator)
            .with_quote_char(serialize_options.quote_char)
            .with_quote_style(serialize_options.quote_style)
            .with_null_value(serialize_options.null)
            .with_line_terminator(serialize_options.line_terminator)
            .with_decimal_comma(serialize_options.decimal_comma);
            
        if let Some(fmt) = serialize_options.date_format { writer = writer.with_date_format(Some(fmt)); }
        if let Some(fmt) = serialize_options.time_format { writer = writer.with_time_format(Some(fmt)); }
        if let Some(fmt) = serialize_options.datetime_format { writer = writer.with_datetime_format(Some(fmt)); }
        if let Some(p) = serialize_options.float_precision { writer = writer.with_float_precision(Some(p)); }
        if let Some(s) = serialize_options.float_scientific { writer = writer.with_float_scientific(Some(s)); }

        if batch_size > 0 {
             writer = writer.with_batch_size(std::num::NonZeroUsize::new(batch_size).unwrap());
        }

        // 3. 执行
        writer.finish(&mut ctx.df)?;
        Ok(())
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_csv(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- CSV Specific Params ---
    include_bom: bool,
    include_header: bool,
    batch_size: usize,
    check_extension: bool,
    // Compression
    compression_code: u8,
    compression_level: i32,
    // Serialize Options
    date_format: *const c_char,
    time_format: *const c_char,
    datetime_format: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
    separator: u8,
    quote_char: u8,
    null_value: *const c_char,
    line_terminator: *const c_char,
    quote_style: u8,
    // --- UnifiedSinkArgs ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    // --- Cloud Params ---
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
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // UnifiedSinkArgs
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close,
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

        // CsvWriterOptions
        let csv_options = unsafe {
            build_csv_writer_options(
                include_bom, include_header, batch_size, check_extension,
                compression_code, compression_level,
                date_format, time_format, datetime_format,
                float_scientific, float_precision, decimal_comma,
                separator, quote_char, null_value, line_terminator, quote_style
            )
        };

        // Format and Target
        let file_format = FileWriteFormat::Csv(csv_options);
        
        let target = SinkTarget::Path(PlRefPath::from(path_str));
        let destination = SinkDestination::File { target };

        // Sink
        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}
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

// ==========================================
// Write&Sink Parquet
// ==========================================
pub(crate) fn build_parquet_write_options(
    compression: u8,        // 0:Uncompressed, 1:Snappy, 2:Gzip, 3:Brotli, 4:Zstd, 5:Lz4Raw
    compression_level: i32, // -1: Default
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    compat_level: i32,      // -1: Default(None), 0: Oldest, 1: Newest
) -> PolarsResult<Arc<ParquetWriteOptions>> {
    
    // 1. Map Compression Options 
    let compression_opts = match compression {
        1 => ParquetCompression::Snappy,
        2 => {
            let lvl = if compression_level >= 0 {
                Some(
                    GzipLevel::try_new(compression_level as u8)
                        .map_err(|_| PolarsError::ComputeError("Invalid Gzip Level".into()))?
                )
            } else {
                None
            };
            ParquetCompression::Gzip(lvl)
        },
        3 => {
            let lvl = if compression_level >= 0 {
                Some(
                    BrotliLevel::try_new(compression_level as u32)
                        .map_err(|_| PolarsError::ComputeError("Invalid Brotli Level".into()))?
                )
            } else {
                None
            };
            ParquetCompression::Brotli(lvl)
        },
        4 => {
            let lvl = if compression_level >= 0 {
                Some(
                    ZstdLevel::try_new(compression_level)
                        .map_err(|_| PolarsError::ComputeError("Invalid Zstd Level".into()))?
                )
            } else {
                None
            };
            ParquetCompression::Zstd(lvl)
        },
        5 => ParquetCompression::Lz4Raw,
        _ => ParquetCompression::Uncompressed,
    };

    // 2. Build StatisticsOptions
    let stats_opts = StatisticsOptions {
        min_value: statistics,
        max_value: statistics,
        distinct_count: statistics,
        null_count: statistics,
    };

    // 3. Handle Compat Level
    let compat_opt = if compat_level < 0 {
        None
    } else {
        Some(CompatLevel::with_level(compat_level as u16)?)
    };

    // 4. Construct Struct and Wrap in Arc
    let options = ParquetWriteOptions {
        compression: compression_opts,
        statistics: stats_opts,
        row_group_size: if row_group_size > 0 { Some(row_group_size) } else { None },
        data_page_size: if data_page_size > 0 { Some(data_page_size) } else { None },
        key_value_metadata: None, 
        arrow_schema: None,
        compat_level: compat_opt,
    };

    Ok(Arc::new(options))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_parquet(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    // --- ParquetWriteOptions ---
    compression_code: u8,
    compression_level: i32, // C# 传入 -1 表示默认
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    _compat_level: i32,     // 忽略
    // --- UnifiedSinkArgs (Eager 忽略) ---
    _maintain_order: bool, _sync_on_close: u8, _mkdir: bool,
    // --- Cloud Options (Eager 忽略) ---
    _cloud_provider: u8, _cloud_retries: usize, _cloud_retry_timeout_ms: u64,
    _cloud_retry_init_backoff_ms: u64, _cloud_retry_max_backoff_ms: u64,
    _cloud_cache_ttl: u64, _cloud_keys: *const *const c_char,
    _cloud_values: *const *const c_char, _cloud_len: usize
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 1. 创建本地文件 (Eager 模式)
        let file = std::fs::File::create(path_str)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create file: {}", e).into()))?;

        // 2. 解析压缩格式和级别
        let compression = match compression_code {
            0 => ParquetCompression::Uncompressed,
            1 => ParquetCompression::Snappy,
            2 => { // Gzip
                // GzipLevel 需要 u8。C# 传 -1 或非法值时回退到 None (Default)
                let lvl = if compression_level >= 0 {
                    GzipLevel::try_new(compression_level as u8).ok()
                } else {
                    None
                };
                ParquetCompression::Gzip(lvl)
            },
            3 => { // Brotli
                // BrotliLevel 需要 u32
                let lvl = if compression_level >= 0 {
                    BrotliLevel::try_new(compression_level as u32).ok()
                } else {
                    None
                };
                ParquetCompression::Brotli(lvl)
            },
            4 => { // Zstd
                // ZstdLevel 需要 i32
                // 注意：Zstd 允许负数级别（极速模式），但在我们的 API 约定中，
                // -1 通常作为 "Default" 哨兵值。
                // 这里为了安全，如果传 -1 我们就用 Default。
                // 如果用户真的想要 Zstd 级别 -1，C# 端可能需要调整逻辑，但目前保持一致性优先。
                let lvl = if compression_level != -1 {
                    ZstdLevel::try_new(compression_level).ok()
                } else {
                    None
                };
                ParquetCompression::Zstd(lvl)
            },
            5 => ParquetCompression::Lz4Raw,
            _ => ParquetCompression::Snappy, // Fallback
        };

        // 3. 统计信息
        let stats_options = if statistics {
            StatisticsOptions::full()
        } else {
            StatisticsOptions::empty()
        };

        // 4. 构建 Writer
        // 注意：ParquetWriter 默认 parallel=true
        let mut writer = ParquetWriter::new(file)
            .with_compression(compression)
            .with_statistics(stats_options);

        // 5. 其他选项
        if row_group_size > 0 {
            writer = writer.with_row_group_size(Some(row_group_size));
        }
        
        if data_page_size > 0 {
            writer = writer.with_data_page_size(Some(data_page_size));
        }

        // 6. 写入
        writer.finish(&mut ctx.df)?;

        Ok(())
    })
}

// 辅助函数：Eager Write 降级
// 使用 ParquetWriteOptions 来配置 Eager Writer
fn fallback_eager_parquet(
    lf: LazyFrame,
    path: &str,
    options: &ParquetWriteOptions // 从 Arc 解包出来传引用
) -> PolarsResult<()> {
    // 1. Collect
    let mut df = lf.collect()?;

    // 2. Create File
    let file = std::fs::File::create(path)
        .map_err(|e| PolarsError::ComputeError(format!("Fallback: failed to create file '{}': {}", path, e).into()))?;

    // 3. Configure Writer
    let mut writer = ParquetWriter::new(file)
        .with_compression(options.compression)
        .with_statistics(options.statistics.clone()) // StatisticsOptions 可能需要 clone
        .set_parallel(true);

    if let Some(s) = options.row_group_size { writer = writer.with_row_group_size(Some(s)); }
    if let Some(s) = options.data_page_size { writer = writer.with_data_page_size(Some(s)); }
    
    // CompatLevel 处理 (如果你的 ParquetWriter 支持 with_compat_level)
    if let Some(_compat) = options.compat_level {
        // writer = writer.with_compat_level(compat); // 取决于 ParquetWriter 实现是否暴露
    }

    // 4. Write
    writer.finish(&mut df)?;

    Ok(())
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- Parquet Options ---
    compression: u8,
    compression_level: i32,
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
    compat_level: i32,
    // --- Unified Options ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    // --- Cloud Params ---
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
        // 1. 准备 LazyFrame
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        let lf = ctx.inner;
        let lf_clone = lf.clone(); // 克隆一份用于 Fallback

        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 2. 构建 Parquet Options (复用 Helper)
        // 返回 Arc<ParquetWriteOptions>
        let write_options_arc = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level
        )?;
        
        // 包装进 FileWriteFormat
        // let file_format = FileWriteFormat::Parquet(write_options_arc.clone());

        // 3. 构建 Unified Args (复用 Helper)
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close,
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

        // 这里的闭包需要移动 parquet_options 的克隆进去
        let options_for_stream = write_options_arc.clone();
        
        let result = catch_unwind(AssertUnwindSafe(|| {
            // A. 构建 Destination
            // 注意：Polars 0.53 这里使用 SinkTarget::Path(PlRefPath)
            let target = SinkTarget::Path(PlRefPath::from(path_str));
            let destination = SinkDestination::File { target };

            // B. 构建 FileFormat
            let file_format = FileWriteFormat::Parquet(options_for_stream);

            // E. 执行
            lf.sink(destination, file_format, unified_args)?
              .collect()
        }));

        // 5. 结果处理与降级 (Fallback)
        match result {
            Ok(Ok(_)) => {
                // 成功
            },
            Ok(Err(e)) => {
                // Polars Error: 可能是 "AnonymousScan" 也就是 Unimplemented
                // 如果是 Cloud 路径，Eager 没法救，直接报错
                if cloud_provider != 0 {
                    return Err(e);
                }
                eprintln!("Streaming Sink failed: {}. Attempting fallback to Eager Write...", e);
                fallback_eager_parquet(lf_clone, path_str, &write_options_arc)?;
            },
            Err(_) => {
                // Panic: 肯定是 AnonymousScan 导致的
                if cloud_provider != 0 {
                    return Err(PolarsError::ComputeError("Streaming sink panicked on Cloud path (Eager fallback not supported for Cloud).".into()));
                }
                eprintln!("Streaming Sink panicked. Falling back to Eager Write...");
                fallback_eager_parquet(lf_clone, path_str, &write_options_arc)?;
            }
        }

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet_partitioned(
    lf_ptr: *mut LazyFrameContext,
    base_path_ptr: *const c_char,
    
    // --- Partition Params ---
    partition_by_ptr: *mut SelectorContext,
    include_keys: bool,
    keys_pre_grouped: bool,
    max_rows_per_file: usize,
    approx_bytes_per_file: u64,

    // --- Parquet Options ---
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,
    compat_level: i32,
    
    // --- Unified Options ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    
    // --- Cloud Params ---
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
        let mut lf_ctx = unsafe { Box::from_raw(lf_ptr) };

        // 1. 获取 Schema (这是为了解析 Partition Selector)
        let schema = lf_ctx.inner.collect_schema()?;

        // 2. 召唤神龙：构建 Partitioned Destination
        // 注意传入后缀 ".parquet"
        let destination = unsafe {
            build_partitioned_destination(
                base_path_ptr,
                ".parquet", 
                &schema,
                partition_by_ptr,
                include_keys,
                keys_pre_grouped,
                max_rows_per_file,
                approx_bytes_per_file
            )?
        };

        // 3. 构建 Parquet Format (复用)
        let write_options_arc = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size,
            compat_level
        )?;
        let file_format = FileWriteFormat::Parquet(write_options_arc);

        // 4. 构建 Unified Args (复用)
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close,
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

        // 5. 发射！
        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}

// ==========================================
// JSON
// ==========================================
// Read JSON (Eager)
unsafe fn apply_json_options<'a, R: MmapBytesReader>(
    mut reader: JsonReader<'a, R>,
    // Params
    columns_ptr: *const *const c_char, 
    columns_len: usize,
    schema_overwrite: Option<&'a Schema>,
    infer_schema_len: *const usize,
    batch_size: *const usize,
    ignore_errors: bool,
    format_code: u8
) -> PolarsResult<JsonReader<'a, R>> {
    
    // 1. Format
    reader = reader.with_json_format(map_json_format(format_code));

    // 2. Schema
    if let Some(schema) = schema_overwrite {
        reader = reader.with_schema_overwrite(schema);
    }

    // 3. Columns (Projection)
    if columns_len > 0 {
        let mut cols = Vec::with_capacity(columns_len);
        for &p in unsafe {std::slice::from_raw_parts(columns_ptr, columns_len)} {
            if !p.is_null() {
                let s = ptr_to_str(p).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
                cols.push(PlSmallStr::from_str(s));
            }
        }
        reader = reader.with_projection(Some(cols));
    }

    // 4. Infer Schema Length
    if !infer_schema_len.is_null() {
        let n = unsafe { *infer_schema_len};
        reader = reader.infer_schema_len(NonZeroUsize::new(n));
    }

    // 5. Batch Size
    if !batch_size.is_null() {
        let n = unsafe {*batch_size};
        if let Some(nz) = NonZeroUsize::new(n) {
            reader = reader.with_batch_size(nz);
        }
    }

    // 6. Ignore Errors
    if ignore_errors {
        reader = reader.with_ignore_errors(true);
    }

    Ok(reader)
}

// ---------------------------------------------------------
// 1. Read JSON (File)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_json(
    path_ptr: *const c_char,
    columns_ptr: *const *const c_char, columns_len: usize,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    batch_size: *const usize,
    ignore_errors: bool,
    format_code: u8
) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::open(path)
            .map_err(|e| PolarsError::ComputeError(format!("File not found: {}", e).into()))?;
        let schema_holder = unsafe { ptr_to_schema_ref(schema_ptr) };
        let schema_ref = schema_holder.as_deref();
        let reader = BufReader::new(file);
        
        let json_reader = JsonReader::new(reader);
        let configured_reader = unsafe { 
            apply_json_options(
                json_reader, 
                columns_ptr, columns_len, 
                schema_ref, infer_schema_len, batch_size, 
                ignore_errors, format_code
            )? 
        };

        let df = configured_reader.finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// ---------------------------------------------------------
// 2. Read JSON (Memory)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_json_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    columns_ptr: *const *const c_char, columns_len: usize,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    batch_size: *const usize,
    ignore_errors: bool,
    format_code: u8
) -> *mut DataFrameContext {
    ffi_try!({
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let cursor = Cursor::new(slice);

        let schema_holder = unsafe { ptr_to_schema_ref(schema_ptr) };
        let schema_ref = schema_holder.as_deref();
        
        let json_reader = JsonReader::new(cursor);
        let configured_reader = unsafe { 
            apply_json_options(
                json_reader, 
                columns_ptr, columns_len, 
                schema_ref, infer_schema_len, batch_size, 
                ignore_errors, format_code
            )? 
        };

        let df = configured_reader.finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

unsafe fn apply_scan_ndjson_options(
    mut reader: LazyJsonLineReader,
    // --- Params ---
    batch_size: *const usize,
    low_memory: bool,
    rechunk: bool,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    n_rows: *const usize,
    ignore_errors: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
) -> PolarsResult<LazyJsonLineReader> {
    
    // 1. Batch Size
    if !batch_size.is_null() {
        let n = unsafe {*batch_size};
        if let Some(nz) = NonZeroUsize::new(n) {
             reader = reader.with_batch_size(Some(nz));
        }
    }

    // 2. Flags
    reader = reader.low_memory(low_memory);
    reader = reader.with_rechunk(rechunk);
    reader = reader.with_ignore_errors(ignore_errors);

    // 3. Schema
    if let Some(schema) = unsafe {ptr_to_schema_ref(schema_ptr)} {
        reader = reader.with_schema_overwrite(Some(schema));
    }

    // 4. Infer Schema Length
    if !infer_schema_len.is_null() {
        let n =unsafe { *infer_schema_len};
        reader = reader.with_infer_schema_length(NonZeroUsize::new(n));
    }

    // 5. N Rows
    if !n_rows.is_null() {
        reader = unsafe {reader.with_n_rows(Some(*n_rows))};
    }

    // 6. Row Index
    if !row_index_name_ptr.is_null() {
        let name = ptr_to_str(row_index_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        reader = reader.with_row_index(Some(RowIndex {
            name: PlSmallStr::from_str(name),
            offset: row_index_offset as u32,
        }));
    }

    // 7. Include Path
    if !include_path_col_ptr.is_null() {
        let name = ptr_to_str(include_path_col_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        reader = reader.with_include_file_paths(Some(PlSmallStr::from_str(name)));
    }

    Ok(reader)
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ndjson(
    path_ptr: *const c_char,
    // --- Options ---
    batch_size: *const usize,
    low_memory: bool,
    rechunk: bool,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    n_rows: *const usize,
    ignore_errors: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let reader = LazyJsonLineReader::new(PlRefPath::new(path));

        let configured_reader = unsafe {
            apply_scan_ndjson_options(
                reader,
                batch_size, low_memory, rechunk, schema_ptr, 
                infer_schema_len, n_rows, ignore_errors, 
                row_index_name_ptr, row_index_offset, include_path_col_ptr
            )?
        };

        let inner = configured_reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner }))) 
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ndjson_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    // --- Options ---
    batch_size: *const usize,
    low_memory: bool,
    rechunk: bool,
    schema_ptr: *mut SchemaContext,
    infer_schema_len: *const usize,
    n_rows: *const usize,
    ignore_errors: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
) -> *mut LazyFrameContext {
    ffi_try!({

        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let buffer = Buffer::from(slice.to_vec()); 
        let sources = ScanSources::Buffers(Arc::new([buffer]));

        let reader = LazyJsonLineReader::new_with_sources(sources);

        let configured_reader = unsafe {
            apply_scan_ndjson_options(
                reader,
                batch_size, low_memory, rechunk, schema_ptr, 
                infer_schema_len, n_rows, ignore_errors, 
                row_index_name_ptr, row_index_offset, include_path_col_ptr
            )?
        };

        let inner = configured_reader.finish()?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}
pub(crate) fn build_ndjson_writer_options(
    compression_code: u8,
    compression_level: i32,
    check_extension: bool,
) -> NDJsonWriterOptions {
    let compression = map_external_compression(compression_code, compression_level);

    NDJsonWriterOptions {
        compression,
        check_extension,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_json(
    df_ptr: *mut DataFrameContext, 
    path: *const c_char,
    json_format: u8 // 0: Json, 1: JsonLines
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let file = File::create(&*p)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let format = map_json_format(json_format);
        
        JsonWriter::new(file)
            .with_json_format(format)
            .finish(&mut ctx.df)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_json(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- NDJson Params ---
    compression_code: u8,
    compression_level: i32,
    check_extension: bool, 
    // --- UnifiedSinkArgs ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
    // --- Cloud Params ---
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
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // UnifiedSinkArgs
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close,
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

        let json_options = build_ndjson_writer_options(
            compression_code,
            compression_level,
            check_extension
        );

        let file_format = FileWriteFormat::NDJson(json_options);
        
        let target = SinkTarget::Path(PlRefPath::from(path_str));
        let destination = SinkDestination::File { target };

        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;

        Ok(())
    })
}

// ==========================================
// IPC
// ==========================================
unsafe fn apply_ipc_options<R: MmapBytesReader>(
    mut reader: IpcReader<R>,
    columns_ptr: *const *const c_char, columns_len: usize,
    n_rows: *const usize,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    rechunk: bool,
    include_path_ptr: *const c_char,
    file_path_value: Option<String>
) -> PolarsResult<IpcReader<R>> {
    
    // 1. Columns
    if columns_len > 0 {
        let mut cols = Vec::with_capacity(columns_len);
        for &p in unsafe {std::slice::from_raw_parts(columns_ptr, columns_len)} {
            if !p.is_null() {
                if let Ok(s) = ptr_to_str(p) {
                    cols.push(s.to_string());
                }
            }
        }
        reader = reader.with_columns(Some(cols));
    }

    // 2. N Rows
    if !n_rows.is_null() {
        reader = unsafe {reader.with_n_rows(Some(*n_rows))};
    }

    // 3. Row Index
    if !row_index_name_ptr.is_null() {
        let name = ptr_to_str(row_index_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        reader = reader.with_row_index(Some(RowIndex {
            name: PlSmallStr::from_str(name),
            offset: row_index_offset as u32,
        }));
    }

    // 4. Include File Path
    if !include_path_ptr.is_null() {
        let col_name = ptr_to_str(include_path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let path_val = file_path_value.unwrap_or_default();

        reader = reader.with_include_file_path(Some((
            PlSmallStr::from_str(col_name),
            PlRefStr::from(path_val.as_str())
        )));
    }

    // 5. Rechunk
    reader = reader.set_rechunk(rechunk);

    Ok(reader)
}

// ---------------------------------------------------------
// 1. Read IPC (File)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_ipc(
    path_ptr: *const c_char,
    columns_ptr: *const *const c_char, columns_len: usize,
    n_rows: *const usize,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    rechunk: bool,
    memory_map: bool,
    include_path_ptr: *const c_char
) -> *mut DataFrameContext {
    ffi_try!({
        let path_str = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let file = File::open(path_str)
            .map_err(|e| PolarsError::ComputeError(format!("File not found: {}", e).into()))?;
        
        let mut reader = IpcReader::new(file);

        if memory_map {
            reader = reader.memory_mapped(Some(PathBuf::from(path_str)));
        }

        reader = unsafe {
            apply_ipc_options(
                reader, 
                columns_ptr, columns_len, 
                n_rows, 
                row_index_name_ptr, row_index_offset, 
                rechunk,
                include_path_ptr,
                Some(path_str.to_string())
            )?
        };

        let df = reader.finish()?;
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// ---------------------------------------------------------
// 2. Read IPC (Memory)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_ipc_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    columns_ptr: *const *const c_char, columns_len: usize,
    n_rows: *const usize,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    rechunk: bool,
    include_path_ptr: *const c_char
) -> *mut DataFrameContext {
    ffi_try!({
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let cursor = Cursor::new(slice);
        
        let reader = IpcReader::new(cursor);
        
        let reader = unsafe {
            apply_ipc_options(
                reader, 
                columns_ptr, columns_len, 
                n_rows, 
                row_index_name_ptr, row_index_offset, 
                rechunk,
                include_path_ptr,
                None
            )?
        };

        let df = reader.finish()?;
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
unsafe fn apply_unified_scan_args(
    // Basic paras
    schema_ptr: *mut SchemaContext,
    n_rows: *const usize,
    rechunk: bool,
    cache: bool,
    // Row Index
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    // Extra
    include_path_col_ptr: *const c_char,
    hive_partitioning: bool,
) -> PolarsResult<UnifiedScanArgs> {
    
    let mut args = UnifiedScanArgs::default();

    // 2. Schema
    if let Some(s) = unsafe { ptr_to_schema_ref(schema_ptr)} {
        args.schema = Some(s);
    }

    // 2. N Rows -> Pre Slice (Enum Variant Correction)
    if !n_rows.is_null() {
        let n = unsafe {*n_rows};
        // Slice is an Enum, use Positive variant
        args.pre_slice = Some(Slice::Positive { 
            offset: 0, 
            len: n // usize
        });
    }

    // 4. Row Index
    if !row_index_name_ptr.is_null() {
        let name = ptr_to_str(row_index_name_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        args.row_index = Some(RowIndex {
            name: PlSmallStr::from_str(name),
            offset: row_index_offset as u32,
        });
    }

    // 5. Flags
    args.rechunk = rechunk;
    args.cache = cache;

    // 6. Include File Path
    if !include_path_col_ptr.is_null() {
        let name = ptr_to_str(include_path_col_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        args.include_file_paths = Some(PlSmallStr::from_str(name));
    }

    // 7. Hive Options
    args.hive_options.enabled = Some(hive_partitioning);

    Ok(args)
}

// ---------------------------------------------------------
// Scan IPC (File)
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ipc(
    path_ptr: *const c_char,
    // --- Unified Args ---
    schema_ptr: *mut SchemaContext,
    n_rows: *const usize,
    rechunk: bool,
    cache: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    hive_partitioning: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let ipc_options = IpcScanOptions::default();

        let unified_args = unsafe {
            apply_unified_scan_args(
                schema_ptr,
                n_rows,
                rechunk,
                cache,
                row_index_name_ptr,
                row_index_offset,
                include_path_col_ptr,
                hive_partitioning
            )?
        };

        // 3. Scan
        let inner = LazyFrame::scan_ipc(
            PlRefPath::new(path), 
            ipc_options, 
            unified_args
        )?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}
// ---------------------------------------------------------
// Scan IPC (Memory) 
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_ipc_memory(
    buffer_ptr: *const u8,
    buffer_len: usize,
    // --- Unified Args ---
    schema_ptr: *mut SchemaContext,
    n_rows: *const usize,
    rechunk: bool,
    cache: bool,
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    hive_partitioning: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        // 1. Deep Copy
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let buffer = Buffer::from(slice.to_vec()); 
        let sources = ScanSources::Buffers(Arc::new([buffer]));

        // 3. Options & Args
        let ipc_options = IpcScanOptions::default();
        let unified_args = unsafe {
            apply_unified_scan_args(
                schema_ptr, n_rows, rechunk, cache,
                row_index_name_ptr, row_index_offset,
                include_path_col_ptr, hive_partitioning
            )?
        };

        // 4. Scan Sources
        let inner = LazyFrame::scan_ipc_sources(
            sources,
            ipc_options,
            unified_args
        )?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}

// ---------------------------------------------------------
// Read & Sink IPC  
// ---------------------------------------------------------

pub(crate) fn build_ipc_write_options(
    compression: u8,        // 0: None, 1: LZ4, 2: ZSTD
    compat_level: i32,      // -1: Newest
    record_batch_size: usize,
    record_batch_statistics: bool,
) -> PolarsResult<IpcWriterOptions> {
    // 1. Map Compression
    let compression = match compression {
        1 => Some(IpcCompression::LZ4),
        2 => {
            // 默认 ZSTD level 3，或者你可以像 Parquet 那样暴露 level 参数
            let level = ZstdLevel::try_new(3)
                .map_err(|_| PolarsError::ComputeError("Invalid ZSTD Level".into()))?;
            Some(IpcCompression::ZSTD(level))
        },
        _ => None,
    };

    // 2. Map Compat Level
    let compat_level = if compat_level < 0 {
        CompatLevel::newest()
    } else {
        CompatLevel::with_level(compat_level as u16)
            .unwrap_or(CompatLevel::newest())
    };

    Ok(IpcWriterOptions {
        compression,
        compat_level,
        record_batch_size: if record_batch_size > 0 { Some(record_batch_size) } else { None },
        record_batch_statistics,
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_ipc(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    // --- IpcWriterOptions params ---
    compression: u8,
    compat_level: i32,
    record_batch_size: usize,      // ✅ Eager Writer 支持这个
    record_batch_statistics: bool, // ✅ Eager Writer 支持这个
    // --- UnifiedSinkArgs params (Eager Writer 忽略) ---
    _maintain_order: bool,
    _sync_on_close: u8,
    _mkdir: bool,
    // --- Cloud Options (Eager Writer 忽略) ---
    _cloud_provider: u8,
    _cloud_retries: usize,
    _cloud_retry_timeout_ms: u64,
    _cloud_retry_init_backoff_ms: u64,
    _cloud_retry_max_backoff_ms: u64,
    _cloud_cache_ttl: u64,
    _cloud_keys: *const *const c_char,
    _cloud_values: *const *const c_char,
    _cloud_len: usize
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 1. 创建本地文件 (Eager 模式核心：直接操作文件系统)
        // 这样就避开了 Lazy 引擎的 AnonymousScan 问题
        let file = std::fs::File::create(path_str)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create file: {}", e).into()))?;

        // 2. 解析 Compression
        let compression_opt = match compression {
            1 => Some(IpcCompression::LZ4),
            2 => {
                let level = ZstdLevel::try_new(3).unwrap();
                Some(IpcCompression::ZSTD(level))
            },
            _ => None,
        };

        // 3. 解析 Compat Level
        let compat = if compat_level < 0 {
            CompatLevel::newest()
        } else {
            CompatLevel::with_level(compat_level as u16)
                .unwrap_or(CompatLevel::newest())
        };

        // 4. 解析 Record Batch Size (0 -> None)
        let batch_size_opt = if record_batch_size > 0 { Some(record_batch_size) } else { None };

        // 5. 配置 Writer 并执行
        // 这里把能用的参数都加上了
        IpcWriter::new(file)
            .with_compression(compression_opt)
            .with_compat_level(compat)
            .with_record_batch_size(batch_size_opt)               // ✅ 加回来了
            .with_record_batch_statistics(record_batch_statistics) // ✅ 加回来了
            .finish(&mut ctx.df)?;

        Ok(())
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_ipc(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // --- IpcWriterOptions params ---
    compression: u8,      
    compat_level: i32,
    record_batch_size: usize,      
    record_batch_statistics: bool, 
    // --- UnifiedSinkArgs params ---
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
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
) {
    ffi_try_void!({
        // 1. Consume LazyFrame & Path
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 2. Build UnifiedSinkArgs
        let unified_args = unsafe {
            build_unified_sink_args(
                mkdir,
                maintain_order,
                sync_on_close,
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

        // 3. Build IpcWriterOptions
        let ipc_options = build_ipc_write_options(
            compression,
            compat_level,
            record_batch_size,
            record_batch_statistics
        )?;

        // 4. Construct FileWriteFormat
        let file_format = FileWriteFormat::Ipc(ipc_options);

        // 5. Construct SinkDestination
        let target = SinkTarget::Path(PlRefPath::from(path_str));
        let destination = SinkDestination::File { target };

        // 6. Execute via Unified Sink Pipeline
        let _ = lf_ctx.inner
            .sink(destination, file_format, unified_args)?
            .collect()?;
        
        Ok(())
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_from_arrow_record_batch(
    c_array_ptr: *mut ffi::ArrowArray, 
    c_schema_ptr: *mut ffi::ArrowSchema
) -> *mut DataFrameContext {
    ffi_try!({
        if c_array_ptr.is_null() || c_schema_ptr.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to pl_from_arrow".into()));
        }

        // Import Arrow Schema
        let field = unsafe { ffi::import_field_from_c(&*c_schema_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))? };
        
        // Import Array
        let arrow_array_struct = unsafe { std::ptr::read(c_array_ptr) };
        let array = unsafe { 
            ffi::import_array_from_c(arrow_array_struct, field.dtype.clone())
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))? 
        };
        
        let df = match array.as_any().downcast_ref::<StructArray>() {
            Some(struct_arr) => {
                let columns: Vec<Column> = struct_arr
                    .values()
                    .iter()
                    .zip(struct_arr.fields())
                    .map(|(arr, field)| {
                        let name = PlSmallStr::from_str(&field.name);
                        
                        Series::from_arrow(name, arr.clone())
                            .map(|s| Column::from(s)) 
                    })
                    .collect::<PolarsResult<Vec<_>>>()?;

                let height = struct_arr.len();
                DataFrame::new(height,columns)?
            },
            None => {
                let name = PlSmallStr::from_str(&field.name);
                let series = Series::from_arrow(name, array)?;
                let height = series.len();

                DataFrame::new(height,vec![Column::from(series)])?
            }
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// ==========================================
// Memory and Convert Ops
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_free_dataframe(ptr: *mut DataFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_to_arrow(
    ctx_ptr: *mut DataFrameContext, 
    out_chunk: *mut ArrowArray, 
    out_schema: *mut ArrowSchema
) {
    ffi_try_void!({
        if ctx_ptr.is_null() {
             return Err(PolarsError::ComputeError("Null pointer passed to pl_to_arrow".into()));
        }
        
        let ctx = unsafe { &mut *ctx_ptr };
        let df = &mut ctx.df;

        let columns = df.columns()
            .iter()
            .map(|s| s.clone().rechunk_to_arrow(CompatLevel::newest()))
            .collect::<Vec<_>>();

        let arrow_schema = df.schema().to_arrow(CompatLevel::newest());
        let fields: Vec<Field> = arrow_schema.iter_values().cloned().collect();

        let struct_array = StructArray::new(
            ArrowDataType::Struct(fields.clone()), 
            df.height(),
            columns,
            None
        );

        unsafe {
            *out_chunk = export_array_to_c(Box::new(struct_array));
            let root_field = Field::new("".into(), ArrowDataType::Struct(fields), false);
            *out_schema = export_field_to_c(&root_field);
        }
        
        Ok(())
    })
}

// ==========================================
// Streaming Sink to DataBase
// ==========================================
// 1. Define Callback
type SinkCallback = extern "C" fn(
    *mut ffi::ArrowArray, 
    *mut ffi::ArrowSchema,
    *mut std::os::raw::c_char
) -> i32;

type CleanupCallback = extern "C" fn(*mut c_void);

// 2. Define Struct 
#[derive(Clone)]
struct CSharpSinkUdf {
    callback: SinkCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void, // GCHandle
}

// Send + Sync
unsafe impl Send for CSharpSinkUdf {}
unsafe impl Sync for CSharpSinkUdf {}

impl Drop for CSharpSinkUdf {
    fn drop(&mut self) {
        (self.cleanup)(self.user_data);
    }
}

impl CSharpSinkUdf {
    fn call(&self, df: DataFrame) -> PolarsResult<DataFrame> {
        // DataFrame -> StructArray (RecordBatch)
        let struct_s = df.into_struct("batch".into()).into_series();
        
        // Get Chunks (Box<dyn Array>)
        let chunks = struct_s.chunks();

        // Error Message Buffer (1KB)
        let mut error_msg_buf = [0u8; 1024]; 
        let error_ptr = error_msg_buf.as_mut_ptr() as *mut std::os::raw::c_char;

        for array_ref in chunks {
            let field = ArrowField::new("".into(), array_ref.dtype().clone(), true);
            let c_array = ffi::export_array_to_c(array_ref.clone());
            let c_schema = ffi::export_field_to_c(&field);

            // Alloc array to heap
            let ptr_array = Box::into_raw(Box::new(c_array));
            let ptr_schema = Box::into_raw(Box::new(c_schema));

            // Call C# Callback
            let status = (self.callback)(ptr_array, ptr_schema, error_ptr);

            if status != 0 {
                let msg = unsafe { CStr::from_ptr(error_ptr).to_string_lossy().into_owned() };
                return Err(PolarsError::ComputeError(format!("C# Sink Failed: {}", msg).into()));
            }
        }

        // Return empty DataFrame
        Ok(DataFrame::empty())
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_map_batches(
    lf_ptr: *mut LazyFrameContext,
    callback: SinkCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // Build UDF Object
        let udf = Arc::new(CSharpSinkUdf { 
            callback, 
            cleanup, 
            user_data 
        });

        // fn map<F>(self, function: F, optimizations: AllowedOptimizations, schema: Option<Arc<dyn UdfSchema>>, name: Option<&'static str>)
        
        let new_lf = lf_ctx.inner.map(
            // 1. function
            move |df| udf.call(df),
            
            // 2. optimizations
            AllowedOptimizations::default(), 

            // 3. schema
            None,

            // 4. name
            Some("csharp_sink"), 
        );

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_export_batches(
    df_ptr: *mut DataFrameContext,
    callback: SinkCallback,
    cleanup: CleanupCallback,
    user_data: *mut c_void
) {
    ffi_try_void!({
        let df_ctx = unsafe { &*df_ptr }; 
        let df = &df_ctx.df;

        let udf = CSharpSinkUdf { 
            callback, 
            cleanup, 
            user_data 
        };

        let struct_s = df.clone().into_struct("batch".into()).into_series();
        let chunks = struct_s.chunks();

        let mut error_msg_buf = [0u8; 1024]; 
        let error_ptr = error_msg_buf.as_mut_ptr() as *mut std::os::raw::c_char;

        for array_ref in chunks {
            let field = ArrowField::new("".into(), array_ref.dtype().clone(), true);
            let c_array = ffi::export_array_to_c(array_ref.clone());
            let c_schema = ffi::export_field_to_c(&field);

            let ptr_array = Box::into_raw(Box::new(c_array));
            let ptr_schema = Box::into_raw(Box::new(c_schema));

            let status = (udf.callback)(ptr_array, ptr_schema, error_ptr);

            if status != 0 {
                return Ok(()); 
            }
        }
        
        Ok(())
    })
}