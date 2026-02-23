use polars::prelude::*;
use polars_io::RowIndex;
use polars_core::prelude::CompatLevel;
use polars_io::cloud::CloudOptions;
use std::os::raw::c_char;
use polars_utils::compression::{ZstdLevel};
use crate::types::{SchemaContext};
use crate::utils::{ ptr_to_schema_ref, ptr_to_str};
use polars_utils::slice_enum::Slice;

#[inline]
pub(crate) unsafe fn apply_unified_scan_args(
    // --- Basic ---
    schema_ptr: *mut SchemaContext,
    n_rows: *const usize,
    rechunk: bool,
    cache: bool,
    glob: bool,
    
    // --- Row Index & Path ---
    row_index_name_ptr: *const c_char,
    row_index_offset: u32,
    include_path_col_ptr: *const c_char,
    
    // --- Hive Partitioning ---
    hive_partitioning: bool,
    hive_schema_ptr: *mut SchemaContext,
    try_parse_hive_dates: bool,
    
    // --- Cloud ---
    cloud_options: Option<CloudOptions>
) -> PolarsResult<UnifiedScanArgs> {
    
    let mut args = UnifiedScanArgs::default();

    // 1. Schema
    if let Some(s) = unsafe { ptr_to_schema_ref(schema_ptr)} {
        args.schema = Some(s);
    }

    // 2. Limit (Pre Slice)
    if !n_rows.is_null() {
        let n = unsafe {*n_rows};
        args.pre_slice = Some(Slice::Positive { 
            offset: 0, 
            len: n 
        });
    }

    // 3. Flags
    args.rechunk = rechunk;
    args.cache = cache;
    args.glob = glob;

    // 4. Row Index
    if !row_index_name_ptr.is_null() {
        let name = ptr_to_str(row_index_name_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        args.row_index = Some(RowIndex {
            name: PlSmallStr::from_str(name),
            offset: row_index_offset as u32,
        });
    }

    // 5. Include File Path
    if !include_path_col_ptr.is_null() {
        let name = ptr_to_str(include_path_col_ptr)
            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))?;
        args.include_file_paths = Some(PlSmallStr::from_str(name));
    }

    // 6. Hive Options
    args.hive_options.enabled = Some(hive_partitioning);
    args.hive_options.try_parse_dates = try_parse_hive_dates;
    if !hive_schema_ptr.is_null() {
        args.hive_options.schema = Some(unsafe { (*hive_schema_ptr).schema.clone() });
    }

    // 7. Cloud Options
    args.cloud_options = cloud_options;

    Ok(args)
}


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