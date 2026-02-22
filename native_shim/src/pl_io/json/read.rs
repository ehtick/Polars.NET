use polars::prelude::*;
use polars_buffer::Buffer;
use std::io::{BufReader,Cursor};
use std::os::raw::c_char;
use std::fs::File;
use crate::pl_io::json::json_utils::{apply_json_options, apply_scan_ndjson_options};
use crate::types::{DataFrameContext, LazyFrameContext, SchemaContext};
use crate::utils::{ptr_to_schema_ref, ptr_to_str};

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