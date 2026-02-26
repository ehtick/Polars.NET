use std::ffi::{c_char,CStr};
use std::fs::File;
use std::io::Cursor;

use polars::error::{PolarsResult,PolarsError};
use polars_io::{SerReader, avro::AvroReader};

use crate::{types::DataFrameContext, utils::ptr_to_str};

// ==========================================
// Read Avro
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_avro(
    path_ptr: *const c_char,
    n_rows_ptr: *const usize,
    columns_ptr: *const *const c_char,
    columns_len: usize,
    projection_ptr: *const usize,
    projection_len: usize,
) -> *mut DataFrameContext {
    ffi_try!({
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let file = File::open(path)
            .map_err(|e| PolarsError::ComputeError(format!("File not found: {}", e).into()))?;

        let mut reader = AvroReader::new(file);

        if !n_rows_ptr.is_null() {
            let n_rows = unsafe { *n_rows_ptr };
            reader = reader.with_n_rows(Some(n_rows));
        }

        if !columns_ptr.is_null() && columns_len > 0 {
            let mut cols = Vec::with_capacity(columns_len);
            for i in 0..columns_len {
                let col_ptr = unsafe { *columns_ptr.add(i) };
                let col_str = unsafe { CStr::from_ptr(col_ptr).to_string_lossy().into_owned() };
                cols.push(col_str);
            }
            reader = reader.with_columns(Some(cols));
        }

        if !projection_ptr.is_null() && projection_len > 0 {
            let proj_slice = unsafe { std::slice::from_raw_parts(projection_ptr, projection_len) };
            reader = reader.with_projection(Some(proj_slice.to_vec()));
        }

        let df = reader.finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// ==========================================
// Read Avro (Memory Buffer)
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_avro_mem(
    buffer_ptr: *const u8,
    buffer_len: usize,
    n_rows_ptr: *const usize,
    columns_ptr: *const *const c_char,
    columns_len: usize,
    projection_ptr: *const usize,
    projection_len: usize,
) -> *mut DataFrameContext {
    ffi_try!({
        if buffer_ptr.is_null() || buffer_len == 0 {
            return Err(PolarsError::ComputeError("Buffer is null or empty".into()));
        }

        let slice = unsafe { std::slice::from_raw_parts(buffer_ptr, buffer_len) };
        
        let cursor = Cursor::new(slice);

        let mut reader = AvroReader::new(cursor);

        if !n_rows_ptr.is_null() {
            let n_rows = unsafe { *n_rows_ptr };
            reader = reader.with_n_rows(Some(n_rows));
        }

        if !columns_ptr.is_null() && columns_len > 0 {
            let mut cols = Vec::with_capacity(columns_len);
            for i in 0..columns_len {
                let col_ptr = unsafe { *columns_ptr.add(i) };
                let col_str = unsafe { CStr::from_ptr(col_ptr).to_string_lossy().into_owned() };
                cols.push(col_str);
            }
            reader = reader.with_columns(Some(cols));
        }

        if !projection_ptr.is_null() && projection_len > 0 {
            let proj_slice = unsafe { std::slice::from_raw_parts(projection_ptr, projection_len) };
            reader = reader.with_projection(Some(proj_slice.to_vec()));
        }

        let df = reader.finish()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}