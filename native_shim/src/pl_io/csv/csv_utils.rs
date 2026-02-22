use polars::prelude::{CsvEncoding, QuoteStyle};

#[inline]
pub(crate) fn map_csv_encoding(encoding: u8) -> CsvEncoding {
    match encoding {
        0 => CsvEncoding::Utf8,
        1 => CsvEncoding::LossyUtf8,
        _ => CsvEncoding::Utf8,
    }
}

#[inline]
pub(crate) fn map_quote_style(code: u8) -> QuoteStyle {
    match code {
        0 => QuoteStyle::Always,
        1 => QuoteStyle::Necessary,
        2 => QuoteStyle::NonNumeric,
        3 => QuoteStyle::Never,
        _ => QuoteStyle::Necessary, // Default
    }
}