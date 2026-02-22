use polars::prelude::*;
use polars_io::mmap::MmapBytesReader;
use polars_io::RowIndex;
use polars_core::prelude::CompatLevel;
use polars_utils::pl_str::PlRefStr;
use std::os::raw::c_char;
use crate::utils::ptr_to_str;
use polars_utils::compression::{ZstdLevel};

pub(crate) unsafe fn apply_ipc_options<R: MmapBytesReader>(
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