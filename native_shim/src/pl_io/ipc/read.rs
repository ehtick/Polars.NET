use polars::prelude::*;
use polars_buffer::Buffer;
use polars_io::RowIndex;
use std::io::Cursor;
use std::os::raw::c_char;
use std::fs::File;
use crate::pl_io::ipc::ipc_utils::apply_ipc_options;
use crate::types::{DataFrameContext, LazyFrameContext, SchemaContext};
use crate::utils::{ ptr_to_schema_ref, ptr_to_str};
use std::path::PathBuf;
use polars_utils::slice_enum::Slice;

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