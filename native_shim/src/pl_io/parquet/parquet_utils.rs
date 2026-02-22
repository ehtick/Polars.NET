use polars::prelude::*;
use polars_core::prelude::CompatLevel;
use polars_utils::compression::{BrotliLevel, GzipLevel, ZstdLevel};

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
