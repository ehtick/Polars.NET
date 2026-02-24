use polars::io::avro::{AvroWriter, AvroCompression};
use std::ffi::{c_char,CStr};
use std::fs::File;

use polars::error::{PolarsResult,PolarsError};
use polars_io::SerWriter;

use crate::pl_io::ffi_buffer::FfiBuffer;
use crate::{types::DataFrameContext, utils::ptr_to_str};
// ==========================================
// Write Avro
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_write_avro(
    df_ptr: *mut DataFrameContext,
    path_ptr: *const c_char,
    compression_type: u8,
    name_ptr: *const c_char,
) {
    ffi_try_void!({
        let ctx = unsafe { &mut *df_ptr };
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;

        let compression = match compression_type {
            1 => Some(AvroCompression::Deflate),
            2 => Some(AvroCompression::Snappy),
            _ => None, 
        };

        let name = if !name_ptr.is_null() {
            unsafe { CStr::from_ptr(name_ptr).to_string_lossy().into_owned() }
        } else {
            String::new()
        };

        let mut file = File::create(path)
            .map_err(|e| PolarsError::ComputeError(format!("Could not create file: {}", e).into()))?;

        AvroWriter::new(&mut file)
            .with_compression(compression)
            .with_name(name)
            .finish(&mut ctx.df)?;
        
        Ok(())
    })
}

// ==========================================
// Write Avro to Buffer
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_write_avro_mem(
    df_ptr: *mut DataFrameContext,
    out_buffer: *mut FfiBuffer,
    compression_type: u8,
    name_ptr: *const c_char,
) {
    ffi_try_void!({
        if df_ptr.is_null() || out_buffer.is_null() {
            return Err(PolarsError::ComputeError("Null pointer passed to avro memory writer".into()));
        }

        let ctx = unsafe { &mut *df_ptr };

        let compression = match compression_type {
            1 => Some(AvroCompression::Deflate),
            2 => Some(AvroCompression::Snappy),
            _ => None,
        };

        let name = if !name_ptr.is_null() {
            unsafe { CStr::from_ptr(name_ptr).to_string_lossy().into_owned() }
        } else {
            String::new()
        };

        let mut mem_writer = Vec::new();

        AvroWriter::new(&mut mem_writer)
            .with_compression(compression)
            .with_name(name)
            .finish(&mut ctx.df)?;

        let mut vec = std::mem::ManuallyDrop::new(mem_writer);

        unsafe {
            (*out_buffer).data = vec.as_mut_ptr();
            (*out_buffer).len = vec.len();
            (*out_buffer).capacity = vec.capacity();
        }
        Ok(())
    })
}