use polars::prelude::*;
use polars_io::mmap::MmapBytesReader;
use polars_io::{HiveOptions, RowIndex};
use polars_arrow::ffi::{self, ArrowArray, ArrowSchema, export_array_to_c, export_field_to_c};
use polars_arrow::array::StructArray;
use polars_arrow::datatypes::{ArrowDataType, Field};
use polars_core::prelude::CompatLevel;
use std::ffi::{CStr, c_void};
use std::io::{BufReader,Cursor};
use std::os::raw::c_char;
use std::fs::File;
use std::num::NonZeroUsize;
use crate::types::{DataFrameContext, LazyFrameContext, SchemaContext};
use crate::utils::{ptr_to_str,ptr_to_opt_string,map_parallel_strategy,ptr_to_schema_ref,map_csv_encoding,map_json_format,map_sync_on_close,ptr_to_vec_string};
use polars_utils::mmap::MemSlice;
use std::path::PathBuf;
use polars_utils::slice_enum::Slice;

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
        let reader = LazyCsvReader::new(PlPath::new(p_ref))
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
    
    // --- 5. Advanced Parsing (Sync with File Scan) ---
    null_values_ptr: *const *const c_char, 
    null_values_len: usize,                
    missing_is_null: bool,                 
    comment_prefix: *const c_char,         
    decimal_comma: bool,                   
) -> *mut LazyFrameContext {
    ffi_try!({
        // --- 1. Prepare Buffer Source ---
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let mem_slice = MemSlice::from_vec(slice.to_vec());
        let sources = ScanSources::Buffers(Arc::new([mem_slice]));

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
        let reader = LazyCsvReader::new_with_sources(sources)
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

        // --- 4. Finish ---
        let inner = reader.finish()?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
    })
}

// ==========================================
// Write & Sink CSV
// ==========================================

fn build_csv_serialize_options(
    // Delimiters
    separator: u8,
    quote_char: u8,
    quote_style: u8,       // 0:Necessary, 1:Always, 2:NonNumeric, 3:Never
    null_value_ptr: *const c_char,
    line_terminator_ptr: *const c_char,
    
    // Formatting
    date_format_ptr: *const c_char,
    time_format_ptr: *const c_char,
    datetime_format_ptr: *const c_char,
    
    // Floats
    float_scientific: i32, // -1:None, 0:False, 1:True
    float_precision: i32,  // -1:None, >=0:Some(usize)
    decimal_comma: bool,
) -> PolarsResult<SerializeOptions> {
    
    // 1. Handle Strings
    let null_value = unsafe { ptr_to_opt_string(null_value_ptr) }.unwrap_or_default();
    
    // line terminator default is "\n"
    let line_terminator = unsafe { ptr_to_opt_string(line_terminator_ptr) }
        .unwrap_or_else(|| "\n".to_string());

    let date_format = unsafe { ptr_to_opt_string(date_format_ptr) };
    let time_format = unsafe { ptr_to_opt_string(time_format_ptr) };
    let datetime_format = unsafe { ptr_to_opt_string(datetime_format_ptr) };

    // 2. Handle QuoteStyle
    let style = match quote_style {
        1 => QuoteStyle::Always,
        2 => QuoteStyle::NonNumeric,
        3 => QuoteStyle::Never,
        _ => QuoteStyle::Necessary,
    };

    // 3. Handle Floats
    let scientific = match float_scientific {
        0 => Some(false),
        1 => Some(true),
        _ => None,
    };

    let precision = if float_precision >= 0 {
        Some(float_precision as usize)
    } else {
        None
    };

    Ok(SerializeOptions {
        date_format,
        time_format,
        datetime_format,
        float_scientific: scientific,
        float_precision: precision,
        decimal_comma,
        separator,
        quote_char,
        null: null_value,
        line_terminator,
        quote_style: style,
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_csv(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    
    // CsvWriter Specific
    has_header: bool,
    use_bom: bool,
    batch_size: usize, // 0 usually means default
    
    // SerializeOptions
    separator: u8,
    quote_char: u8,
    quote_style: u8,
    null_value_ptr: *const c_char,
    line_terminator_ptr: *const c_char,
    date_format_ptr: *const c_char,
    time_format_ptr: *const c_char,
    datetime_format_ptr: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create file: {}", e).into()))?;

        // 1. Build SerializeOptions
        let options = build_csv_serialize_options(
            separator,
            quote_char,
            quote_style,
            null_value_ptr,
            line_terminator_ptr,
            date_format_ptr,
            time_format_ptr,
            datetime_format_ptr,
            float_scientific,
            float_precision,
            decimal_comma
        )?;

        let mut writer = CsvWriter::new(file)
            .include_header(has_header)
            .include_bom(use_bom);
            
        writer = writer
            .with_separator(options.separator)
            .with_quote_char(options.quote_char)
            .with_quote_style(options.quote_style)
            .with_null_value(options.null)
            .with_line_terminator(options.line_terminator)
            .with_decimal_comma(options.decimal_comma);
            
        if let Some(fmt) = options.date_format { writer = writer.with_date_format(Some(fmt)); }
        if let Some(fmt) = options.time_format { writer = writer.with_time_format(Some(fmt)); }
        if let Some(fmt) = options.datetime_format { writer = writer.with_datetime_format(Some(fmt)); }
        if let Some(p) = options.float_precision { writer = writer.with_float_precision(Some(p)); }
        if let Some(s) = options.float_scientific { writer = writer.with_float_scientific(Some(s)); }

        if batch_size > 0 {
            writer = writer.with_batch_size(std::num::NonZeroUsize::new(batch_size).unwrap());
        }

        writer.finish(&mut ctx.df)?;
            
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_csv(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    
    // CsvWriterOptions specific
    include_header: bool,
    include_bom: bool,
    batch_size: usize,
    
    // SinkOptions specific
    maintain_order: bool,
    sync_on_close: u8,    // 0: None, 1: Data, 2: All
    mkdir: bool,

    // SerializeOptions (pass-through to utils)
    separator: u8,
    quote_char: u8,
    quote_style: u8,
    null_value_ptr: *const c_char,
    line_terminator_ptr: *const c_char,
    date_format_ptr: *const c_char,
    time_format_ptr: *const c_char,
    datetime_format_ptr: *const c_char,
    float_scientific: i32,
    float_precision: i32,
    decimal_comma: bool,
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // Build SerializeOptions
        let serialize_options = build_csv_serialize_options(
            separator,
            quote_char,
            quote_style,
            null_value_ptr,
            line_terminator_ptr,
            date_format_ptr,
            time_format_ptr,
            datetime_format_ptr,
            float_scientific,
            float_precision,
            decimal_comma
        )?;

        // Set Batch Size
        let default_options = CsvWriterOptions::default();
        
        let final_batch_size = NonZeroUsize::new(batch_size)
            .unwrap_or(default_options.batch_size);

        // Build CsvWriterOptions
        let writer_options = CsvWriterOptions {
            serialize_options,
            include_header,       
            include_bom,         
            batch_size: final_batch_size
        };

        // Build SinkOptions 
        let sink_options = SinkOptions {
            maintain_order,
            sync_on_close: map_sync_on_close(sync_on_close),
            mkdir,
            ..Default::default()
        };

        // Execuate Sink
        let target = SinkTarget::Path(PlPath::new(path_str));

        let _ = lf_ctx.inner.sink_csv(
            target, 
            writer_options, 
            None, // cloud_options
            sink_options
        )?
        .with_new_streaming(true)
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

fn build_scan_args(
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
    try_parse_hive_dates: bool             
) -> ScanArgsParquet {
    let mut args = ScanArgsParquet::default();

    if !n_rows.is_null() { unsafe { args.n_rows = Some(*n_rows); } }
    args.parallel = map_parallel_strategy(parallel_code);
    args.low_memory = low_memory;
    args.use_statistics = use_statistics;
    args.glob = glob;
    args.allow_missing_columns = allow_missing_columns;

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
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- New Args ---
    schema_ptr: *mut SchemaContext,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, allow_missing_columns, row_index_name_ptr, 
            row_index_offset, include_path_col_ptr,
            schema_ptr, hive_schema_ptr, try_parse_hive_dates 
        );

        let lf = LazyFrame::scan_parquet(PlPath::new(path), args)?;
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
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    // --- New Args ---
    schema_ptr: *mut SchemaContext,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool
) -> *mut LazyFrameContext {
    ffi_try!({
        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let vec_data = slice.to_vec(); 
        let mem_slice = MemSlice::from_vec(vec_data);
        let sources = ScanSources::Buffers(Arc::new([mem_slice]));

        let args = build_scan_args(
            n_rows, parallel_code, low_memory, use_statistics, 
            glob, allow_missing_columns, row_index_name_ptr, 
            row_index_offset, include_path_col_ptr,
            schema_ptr, hive_schema_ptr, try_parse_hive_dates 
        );

        let lf = LazyFrame::scan_parquet_sources(sources, args)?;
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner:lf })))
    })
}

// ==========================================
// Write&Sink Parquet
// ==========================================
fn build_parquet_write_options(
    compression: u8,        // 0:Uncompressed, 1:Snappy, 2:Gzip, 3:Brotli, 4:Zstd, 5:Lz4Raw
    compression_level: i32, // -1: Default
    statistics: bool,
    row_group_size: usize,
    data_page_size: usize,
) -> PolarsResult<ParquetWriteOptions> {
    // 1. Map Compression Options 
    let compression_opts = match compression {
        1 => ParquetCompression::Snappy,
        2 => {
            // Gzip (level is u8)
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
            // Brotli (level is u32)
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
            // Zstd (level is i32)
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

    // 3. Construct ParquetWriteOptions
    Ok(ParquetWriteOptions {
        compression: compression_opts,
        statistics: stats_opts,
        row_group_size: if row_group_size > 0 { Some(row_group_size) } else { None },
        data_page_size: if data_page_size > 0 { Some(data_page_size) } else { None },
        key_value_metadata: None,
        field_overwrites: vec![], 
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_parquet(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,  
    parallel: bool,
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create file: {}", e).into()))?;

        let options = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size
        )?;

        // Config Writer 
        let mut writer = ParquetWriter::new(file)
            .with_compression(options.compression)
            .with_statistics(options.statistics)
            .set_parallel(parallel);

        if let Some(s) = options.row_group_size {
            writer = writer.with_row_group_size(Some(s));
        }
        
        if let Some(s) = options.data_page_size {
            writer = writer.with_data_page_size(Some(s));
        }

        writer.finish(&mut ctx.df)?;
            
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_parquet(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // ParquetWriteOptions 
    compression: u8,        
    compression_level: i32, 
    statistics: bool,       
    row_group_size: usize,  
    data_page_size: usize,
    // SinkOptions 
    maintain_order: bool,
    sync_on_close: u8,
    mkdir: bool,
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // ParquetWriteOptions
        let write_options = build_parquet_write_options(
            compression,
            compression_level,
            statistics,
            row_group_size,
            data_page_size
        )?;

        // SinkOptions
        let sink_options = SinkOptions {
            maintain_order,
            sync_on_close: map_sync_on_close(sync_on_close),
            mkdir,
            ..Default::default()
        };

        let target = SinkTarget::Path(PlPath::new(path_str));

        let _ = lf_ctx.inner
            .sink_parquet(
                target, 
                write_options, 
                None, // cloud_options
                sink_options
            )?
            .with_new_streaming(true)
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
        
        let reader = LazyJsonLineReader::new(PlPath::new(path));

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
        let vec_data = slice.to_vec(); 
        let mem_slice = MemSlice::from_vec(vec_data);
        
        let sources = ScanSources::Buffers(Arc::new([mem_slice]));

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
    // JsonWriterOptions
    json_format: u8,      // 0: Json(not available at Polars 0.52), 1: JsonLines
    // SinkOptions
    maintain_order: bool,
    sync_on_close: u8,    // 0: None, 1: Data, 2: All
    mkdir: bool,
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 1. Build JsonWriterOptions
        let _ = map_json_format(json_format);

        let writer_options = JsonWriterOptions {
            ..Default::default()
        };

        // 2. Build SinkOptions
        let sink_options = SinkOptions {
            maintain_order,
            sync_on_close: map_sync_on_close(sync_on_close),
            mkdir,
            ..Default::default()
        };

        // 3. Target
        let target = SinkTarget::Path(PlPath::new(path_str));

        // 4. Execute
        let _ = lf_ctx.inner.sink_json(
            target, 
            writer_options, 
            None, 
            sink_options
        )?
        .with_new_streaming(true)
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
            Arc::from(path_val.as_str())
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
            PlPath::new(path), 
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
        let vec_data = slice.to_vec();
        let mem_slice = MemSlice::from_vec(vec_data);
        
        // 2. Construct Sources
        let sources = ScanSources::Buffers(Arc::new([mem_slice]));

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

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_ipc(
    df_ptr: *mut DataFrameContext,
    path: *const c_char,
    compression: u8, // 0: None, 1: LZ4, 2: ZSTD
    parallel: bool,
    compat_level: i32,
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let file = File::create(&*p)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        let compression_opt = match compression {
            1 => Some(IpcCompression::LZ4),
            2 => {
                let level = polars_utils::compression::ZstdLevel::try_new(3).unwrap();
                Some(IpcCompression::ZSTD(level))
            },
            _ => None,
        };

        let compat = if compat_level < 0 {
            CompatLevel::newest()
        } else {
            CompatLevel::with_level(compat_level as u16)
                .unwrap_or(CompatLevel::newest())
        };

        IpcWriter::new(file)
            .with_compression(compression_opt)
            .with_parallel(parallel)
            .with_compat_level(compat)
            .finish(&mut ctx.df)
    })
}


#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sink_ipc(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char,
    // IpcWriterOptions params
    compression: u8,      // 0: None, 1: LZ4, 2: ZSTD
    compat_level: i32,    // -1: Newest
    // SinkOptions params
    maintain_order: bool,
    sync_on_close: u8,    // 0: None, 1: Data, 2: All
    mkdir: bool,
) {
    ffi_try_void!({
        // 1. Consume the pointer (LazyFrame operations consume self)
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path = ptr_to_str(path_ptr).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        // 2. Build IpcWriterOptions (Reuse logic from IO)
        let compression_opt = match compression {
            1 => Some(IpcCompression::LZ4),
            2 => {
                let level = polars_utils::compression::ZstdLevel::try_new(3).unwrap();
                Some(IpcCompression::ZSTD(level))
            },
            _ => None,
        };

        let compat = if compat_level < 0 {
            CompatLevel::newest()
        } else {
            CompatLevel::with_level(compat_level as u16)
                .unwrap_or(CompatLevel::newest())
        };

        let writer_options = IpcWriterOptions {
            compression: compression_opt,
            compat_level: compat,
            ..Default::default()
        };

        let sink_options = SinkOptions {
            maintain_order,
            sync_on_close: map_sync_on_close(sync_on_close),
            mkdir,
            ..Default::default()
        };

        // 4. Build Target
        let target = SinkTarget::Path(PlPath::new(path)); // PlPath usually maps to PathBuf internally in Polars IO

        // 5. Execute Sink
        // sink_ipc returns a LazyFrame that represents the sink operation.
        // We then call collect() to actually execute the streaming write.
        let _ = lf_ctx.inner
            .sink_ipc(
                target, 
                writer_options, 
                None, // CloudOptions (Future work)
                sink_options
            )?
            .with_new_streaming(true) // Ensure streaming engine is used
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
                
                DataFrame::new(columns)?
            },
            None => {
                let name = PlSmallStr::from_str(&field.name);
                let series = Series::from_arrow(name, array)?;
                
                DataFrame::new(vec![Column::from(series)])?
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

        let columns = df.get_columns()
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