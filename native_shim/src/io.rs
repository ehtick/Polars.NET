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
use crate::utils::{ptr_to_str,map_parallel_strategy,ptr_to_schema_ref,map_csv_encoding,map_json_format};
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

        // --- Option ptr ---
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

        // --- ParseOptions ---
        let parse_options = CsvParseOptions {
            separator,
            try_parse_dates,
            encoding: csv_encoding,
            ..Default::default()
        };

        // ---  ReadOptions ---
        let read_options = CsvReadOptions {
            has_header,
            skip_rows,
            n_rows,
            infer_schema_length,
            ignore_errors,
            low_memory,
            columns, 
            schema_overwrite,
            parse_options: Arc::new(parse_options),
            ..Default::default() 
        };

        // --- Execuate ---
        let df = read_options
            .try_into_reader_with_file_path(Some(p.into_owned().into()))?
            .finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_scan_csv(
    path: *const c_char,
    
    // Core Configs
    has_header: bool,
    separator: u8,
    ignore_errors: bool,
    try_parse_dates: bool,
    low_memory: bool,
    cache: bool,
    rechunk: bool,

    // Sizes
    skip_rows: usize,
    n_rows_ptr: *const usize,
    infer_schema_len_ptr: *const usize,

    // Row Index
    row_index_name: *const c_char,
    row_index_offset: usize,

    // Schema & Encoding
    schema_ptr: *mut SchemaContext,
    encoding: u8, 
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

        // Build Reader
        let reader = LazyCsvReader::new(PlPath::new(p_ref))
            .with_has_header(has_header)
            .with_separator(separator)
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
            .with_row_index(row_index); 

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

    // --- Core Configs---
    has_header: bool,
    separator: u8,
    ignore_errors: bool,
    try_parse_dates: bool,
    low_memory: bool,
    cache: bool,
    rechunk: bool,

    // --- Sizes ---
    skip_rows: usize,
    n_rows_ptr: *const usize,
    infer_schema_len_ptr: *const usize,

    // --- Row Index---
    row_index_name: *const c_char,
    row_index_offset: usize,

    // --- 5. Schema & Encoding ---
    schema_ptr: *mut SchemaContext,
    encoding: u8, 
) -> *mut LazyFrameContext {
    ffi_try!({

        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        let mem_slice = MemSlice::from_vec(slice.to_vec());
        
        let sources = ScanSources::Buffers(Arc::new([mem_slice]));

        let csv_encoding = crate::utils::map_csv_encoding(encoding);

        let n_rows = if n_rows_ptr.is_null() { None } else { Some(unsafe { *n_rows_ptr }) };
        let infer_schema_length = if infer_schema_len_ptr.is_null() { None } else { Some(unsafe { *infer_schema_len_ptr }) };
        
        let schema_overwrite = if schema_ptr.is_null() {
            None
        } else {
            Some(unsafe { &*schema_ptr }.schema.clone())
        };

        let row_index = if row_index_name.is_null() {
            None
        } else {
            let name_cow = unsafe { CStr::from_ptr(row_index_name).to_string_lossy() };
            Some(RowIndex {
                name: PlSmallStr::from_str(name_cow.as_ref()),
                offset: row_index_offset as u32,
            })
        };

        let reader = LazyCsvReader::new_with_sources(sources)
            .with_has_header(has_header)
            .with_separator(separator)
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
            .with_row_index(row_index);

        let inner = reader.finish()?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner })))
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
// 2. Read Parquet (Memory / Bytes)
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
// 2. Scan Parquet (Memory / Buffers)
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

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_ipc(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path = ptr_to_str(path_ptr).unwrap();

        // Prepare Option
        let writer_options = IpcWriterOptions::default();
        let sink_options = SinkOptions::default();

        // Build Target
        let target = SinkTarget::Path(PlPath::new(path));

        // Call sink_ipc
        // target, options, cloud_options, sink_options
        let sink_lf = lf_ctx.inner.sink_ipc(
            target, 
            writer_options, 
            None, // CloudOptions
            sink_options
        )?;

        
        let _ = sink_lf
        .with_new_streaming(true)
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
// Write Ops
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_write_csv(df_ptr: *mut DataFrameContext, path_ptr: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let mut file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create file: {}", e).into()))?;

        CsvWriter::new(&mut file)
            .finish(&mut ctx.df)?;
        
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_write_parquet(df_ptr: *mut DataFrameContext, path_ptr: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create file: {}", e).into()))?;

        ParquetWriter::new(file)
            .finish(&mut ctx.df)?;
            
        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_ipc(df_ptr: *mut DataFrameContext, path: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let file = File::create(&*p).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        IpcWriter::new(file)
            .finish(&mut ctx.df)
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_write_json(df_ptr: *mut DataFrameContext, path: *const c_char) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let p = unsafe { CStr::from_ptr(path).to_string_lossy() };
        
        let file = File::create(&*p).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        
        JsonWriter::new(file)
        .with_json_format(JsonFormat::Json)
        .finish(&mut ctx.df)
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

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_parquet(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).unwrap();

        let pl_path = PlPath::new(path_str);
        let target = SinkTarget::Path(pl_path.into());

        let write_options = ParquetWriteOptions::default();
        let sink_options = SinkOptions::default();

        let sink_lf = lf_ctx.inner.sink_parquet(
            target, 
            write_options, 
            None, // cloud_options
            sink_options
        )?;

        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_json(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).unwrap();
        let pl_path = PlPath::new(path_str);
        
        let target = SinkTarget::Path(pl_path.into());
        let writer_options = JsonWriterOptions::default();
        let sink_options = SinkOptions::default();

        let sink_lf = lf_ctx.inner.sink_json(
            target, 
            writer_options, 
            None, 
            sink_options
        )?;
        
        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;

        Ok(())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_sink_csv(
    lf_ptr: *mut LazyFrameContext,
    path_ptr: *const c_char
) {
    ffi_try_void!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let path_str = ptr_to_str(path_ptr).unwrap();
        let pl_path = PlPath::new(path_str);
        
        let target = SinkTarget::Path(pl_path.into());
        let writer_options = CsvWriterOptions::default();
        let sink_options = SinkOptions::default();

        let sink_lf = lf_ctx.inner.sink_csv(
            target, 
            writer_options, 
            None, 
            sink_options
        )?;
        
        let _ = sink_lf
        .with_new_streaming(true)
        .collect()?;

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