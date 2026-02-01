use polars::prelude::*;
use calamine::{open_workbook_auto, Reader, DataType as ExcelDataType, Data};
use chrono::{NaiveDate};
use crate::types::{DataFrameContext,SchemaContext}; 
use crate::utils::{ptr_to_str, ptr_to_schema_ref}; 

// ---------------------------------------------------------
// Helpers：Convert Calamine Time to Polars AnyValue
// ---------------------------------------------------------
fn convert_excel_datetime(ed: &calamine::ExcelDateTime) -> AnyValue<'static> {
    match ed.as_datetime() {
        Some(dt) => {
            let micros = dt.and_utc().timestamp_micros();
            AnyValue::Datetime(micros, TimeUnit::Microseconds, None)
        },
        None => AnyValue::Null,
    }
}

fn convert_excel_duration(ed: &calamine::ExcelDateTime) -> AnyValue<'static> {
    match ed.as_duration() {
        Some(d) => {
             let micros = d.num_microseconds().unwrap_or(0);
             AnyValue::Duration(micros, TimeUnit::Microseconds)
        },
        None => AnyValue::Null
    }
}

// ---------------------------------------------------------
// Core Logic
// ---------------------------------------------------------
pub fn read_excel_calamine(
    path: &str, 
    sheet_name: Option<&str>, 
    sheet_idx: usize,
    has_header: bool,
    schema_overrides: Option<SchemaRef>, 
    _infer_schema_len: usize,
    drop_empty_rows: bool,
    raise_if_empty: bool,
) -> PolarsResult<DataFrame> {
    
    // Open Workbook
    let mut workbook = open_workbook_auto(path)
        .map_err(|e| PolarsError::ComputeError(format!("Cannot open Excel file: {}", e).into()))?;

    // Get Range
    let range_result_opt = match sheet_name {
        Some(name) => Some(workbook.worksheet_range(name)),
        None => workbook.worksheet_range_at(sheet_idx),
    };

    let range = range_result_opt
        .ok_or_else(|| PolarsError::ComputeError("Sheet not found".into()))?
        .map_err(|e| PolarsError::ComputeError(format!("Error parsing sheet: {}", e).into()))?;

    let (height, width) = range.get_size();

    // Check Empty
    if height == 0 || width == 0 {
        if raise_if_empty {
            return Err(PolarsError::ComputeError("Sheet is empty".into()));
        } else {
            return Ok(DataFrame::empty());
        }
    }

    let mut rows_iter = range.rows();
    
    // Parse Header
    let headers: Vec<String> = if has_header {
        if let Some(first_row) = rows_iter.next() {
            first_row.iter()
                .map(|cell| cell.to_string())
                .collect()
        } else {
            return Ok(DataFrame::empty());
        }
    } else {
        (0..width).map(|i| format!("Column_{}", i)).collect()
    };

    // Schema (Column Index -> Target DataType)
    let mut col_type_overrides: Vec<Option<DataType>> = vec![None; width];
    if let Some(schema) = &schema_overrides {
        for (i, name) in headers.iter().enumerate() {
            if let Some(dtype) = schema.get(name) {
                col_type_overrides[i] = Some(dtype.clone());
            }
        }
    }

    // Rows to Columns 
    let mut columns_data: Vec<Vec<AnyValue>> = vec![Vec::with_capacity(height); width];

    for row in rows_iter {
        // Drop Empty Rows
        if drop_empty_rows && row.iter().all(|c| c.is_empty()) {
            continue;
        }

        for (col_idx, cell) in row.iter().enumerate() {
            if col_idx >= width { break; }

            let target_type = &col_type_overrides[col_idx];

            // -------------------------------------------------------------
            // Cell Value vs Schema Target
            // -------------------------------------------------------------
            let val = match (cell, target_type) {
                // -------------------------------------------------------------
                // Schema Overrides
                // -------------------------------------------------------------
                
                // To String
                (Data::Int(v), Some(DataType::String)) => AnyValue::StringOwned(v.to_string().into()),
                (Data::Float(v), Some(DataType::String)) => AnyValue::StringOwned(v.to_string().into()),
                (Data::Bool(v), Some(DataType::String)) => AnyValue::StringOwned(v.to_string().into()),
                (Data::DateTime(ed), Some(DataType::String)) => AnyValue::StringOwned(ed.to_string().into()),
                (Data::DateTimeIso(s), Some(DataType::String)) => AnyValue::String(s.as_str()),

                // Float but Date/Datetime (Excel OADate)
                (Data::Float(v), Some(DataType::Datetime(_, _))) => {
                    let ed = calamine::ExcelDateTime::new(*v, calamine::ExcelDateTimeType::DateTime, false);
                    convert_excel_datetime(&ed)
                },
                (Data::Float(v), Some(DataType::Date)) => {
                     let ed = calamine::ExcelDateTime::new(*v, calamine::ExcelDateTimeType::DateTime, false);
                     match ed.as_datetime() {
                         Some(dt) => {
                             let days = (dt.date() - NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32;
                             AnyValue::Date(days)
                         },
                         None => AnyValue::Null
                     }
                },

                // String to Numeric
                (Data::String(s), Some(DataType::Int64)) => s.parse::<i64>().map(AnyValue::Int64).unwrap_or(AnyValue::Null),
                (Data::String(s), Some(DataType::Float64)) => s.parse::<f64>().map(AnyValue::Float64).unwrap_or(AnyValue::Null),

                // -------------------------------------------------------------
                // B. AutoDetect
                // -------------------------------------------------------------
                
                (Data::Int(v), _) => AnyValue::Int64(*v),
                (Data::Float(v), _) => AnyValue::Float64(*v),
                (Data::String(v), _) => AnyValue::String(v.as_str()),
                (Data::Bool(v), _) => AnyValue::Boolean(*v),
                
                (Data::DateTime(ed), _) => {
                    if ed.is_duration() { convert_excel_duration(ed) } else { convert_excel_datetime(ed) }
                },

                // ISO String
                (Data::DateTimeIso(s), _) => AnyValue::String(s.as_str()),
                (Data::DurationIso(s), _) => AnyValue::String(s.as_str()),

                // Nulls
                // (Data::Error(_), _) | (Data::Empty, _) => AnyValue::Null,
                
                // FallBack
                _ => AnyValue::Null,
            };

            columns_data[col_idx].push(val);
        }

        for col_idx in row.len()..width {
             columns_data[col_idx].push(AnyValue::Null);
        }
    }

    // Build Series
    let mut series_vec = Vec::with_capacity(width);
    for ((name, col_vals), override_type) in headers.into_iter().zip(columns_data.into_iter()).zip(col_type_overrides) {
        
        let mut s = Series::from_any_values(PlSmallStr::from_str(&name), &col_vals, true)
            .map_err(|e| PolarsError::ComputeError(format!("Series error column {}: {}", name, e).into()))?;

        if let Some(target_dtype) = override_type {
            if s.dtype() != &target_dtype {
                if s.dtype() == &DataType::Null {
                    s = s.cast(&target_dtype)?;
                } else {
                    s = s.cast(&target_dtype)?;
                }
            }
        }

        series_vec.push(s.into());
    }

    DataFrame::new(series_vec)
}

// ---------------------------------------------------------
// FFI Export
// ---------------------------------------------------------
#[unsafe(no_mangle)]
pub extern "C" fn pl_read_excel(
    path_ptr: *const std::ffi::c_char,
    sheet_name_ptr: *const std::ffi::c_char,
    sheet_index: usize,
    schema_ptr: *mut SchemaContext,
    has_header: bool,
    infer_schema_len: usize,
    drop_empty_rows: bool,
    raise_if_empty: bool
) -> *mut DataFrameContext {
    ffi_try!({
        // String conversion
        let path = ptr_to_str(path_ptr)
            .map_err(|e| PolarsError::ComputeError(format!("Invalid path string: {}", e).into()))?;

        let sheet_name = if !sheet_name_ptr.is_null() {
            Some(ptr_to_str(sheet_name_ptr)
                .map_err(|e| PolarsError::ComputeError(format!("Invalid sheet name: {}", e).into()))?)
        } else {
            None
        };

        // Get Schema
        let schema_overrides = unsafe { ptr_to_schema_ref(schema_ptr) };

        // Read Excel
        let df = read_excel_calamine(
            path, 
            sheet_name, 
            sheet_index, 
            has_header,
            schema_overrides, 
            infer_schema_len,
            drop_empty_rows,
            raise_if_empty
        )?;

        // Return Ptr
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}