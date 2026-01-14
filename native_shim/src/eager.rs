use polars::prelude::pivot::pivot_stable;
use polars::prelude::*;
use polars_core::utils::concat_df;
use polars_arrow::ffi::{ArrowArrayStreamReader,ArrowArrayStream};
use polars_arrow::array::Array;
use std::ffi::CStr;
use std::{ffi::CString, os::raw::c_char};
use crate::types::*;
use polars::lazy::dsl::UnpivotArgsDSL;
use polars::functions::{concat_df_horizontal,concat_df_diagonal};
use polars::prelude::{Field as PolarsField};
use crate::utils::{consume_exprs_array, map_jointype, ptr_to_str};

// ==========================================
// 0. Memory Safety
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_free(ptr: *mut DataFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

// ==========================================
// Macro Define
// ==========================================

/// Mode A: DataFrame -> Single Expr -> DataFrame
macro_rules! gen_eager_op_single {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            df_ptr: *mut DataFrameContext, 
            expr_ptr: *mut ExprContext
        ) -> *mut DataFrameContext {
            ffi_try!({
                let ctx = unsafe { &mut *df_ptr };
                let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
                
                // Ops: clone -> lazy -> op -> collect
                let res_df = ctx.df.clone().lazy()
                    .$method(expr_ctx.inner)
                    .collect()?;

                Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
            })
        }
    };
}

/// Mode B: DataFrame -> Expr array -> DataFrame
macro_rules! gen_eager_op_vec {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            df_ptr: *mut DataFrameContext, 
            exprs_ptr: *const *mut ExprContext, 
            len: usize
        ) -> *mut DataFrameContext {
            ffi_try!({
                let ctx = unsafe { &mut *df_ptr };
                let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };
                
                let res_df = ctx.df.clone().lazy()
                    .$method(exprs)
                    .collect()?;

                Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
            })
        }
    };
}

// ==========================================
// Macro Apply
// ==========================================

gen_eager_op_single!(pl_filter, filter);

gen_eager_op_vec!(pl_select, select);
gen_eager_op_vec!(pl_with_columns, with_columns);

// ==========================================
// GroupBy
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_groupby_agg(
    df_ptr: *mut DataFrameContext,
    by_ptr: *const *mut ExprContext, by_len: usize,
    agg_ptr: *const *mut ExprContext, agg_len: usize
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &mut *df_ptr };
        
        let by_exprs = unsafe { consume_exprs_array(by_ptr, by_len) };
        let agg_exprs = unsafe { consume_exprs_array(agg_ptr, agg_len) };

        let res_df = ctx.df.clone().lazy()
            .group_by_stable(by_exprs)
            .agg(agg_exprs)
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

// ==========================================
// Join
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_join(
    left_ptr: *mut DataFrameContext,
    right_ptr: *mut DataFrameContext,
    left_on_ptr: *const *mut ExprContext, left_on_len: usize,
    right_on_ptr: *const *mut ExprContext, right_on_len: usize,
    how_code: i32
) -> *mut DataFrameContext {
    ffi_try!({
        let left_ctx = unsafe { &*left_ptr };
        let right_ctx = unsafe { &*right_ptr };

        let how = map_jointype(how_code);

        let left_on = unsafe { consume_exprs_array(left_on_ptr, left_on_len) };
        let right_on = unsafe { consume_exprs_array(right_on_ptr, right_on_len) };

        let args = JoinArgs::new(how);
        
        let res_df = left_ctx.df.clone().lazy()
            .join(right_ctx.df.clone().lazy(), left_on, right_on, args)
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
// ==========================================
// Sort
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_sort(
    df_ptr: *mut DataFrameContext,
    expr_ptrs: *const *mut ExprContext, 
    expr_len: usize,
    descending_ptr: *const bool,        
    descending_len: usize,
    nulls_last_ptr: *const bool,        
    nulls_last_len: usize,
    maintain_order: bool                
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let mut exprs = Vec::with_capacity(expr_len);
        let ptr_slice = unsafe { std::slice::from_raw_parts(expr_ptrs, expr_len) };
        for &ptr in ptr_slice {
            let expr_ctx = unsafe { Box::from_raw(ptr) };
            exprs.push(expr_ctx.inner);
        }

        let desc_slice = unsafe { std::slice::from_raw_parts(descending_ptr, descending_len) };
        let descending = if descending_len == 1 && expr_len > 1 {
            vec![desc_slice[0]; expr_len]
        } else {
            desc_slice.to_vec()
        };

        let nulls_slice = unsafe { std::slice::from_raw_parts(nulls_last_ptr, nulls_last_len) };
        let nulls_last = if nulls_last_len == 1 && expr_len > 1 {
            vec![nulls_slice[0]; expr_len]
        } else {
            nulls_slice.to_vec()
        };

        let options = SortMultipleOptions::default()
            .with_order_descending_multi(descending)
            .with_nulls_last_multi(nulls_last)
            .with_maintain_order(maintain_order);

        let res_df = ctx.df.clone()
            .lazy()
            .sort_by_exprs(exprs, options)
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
// ==========================================
// DataFrame Ops
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_height(df_ptr: *mut DataFrameContext) -> usize {
    let ctx = unsafe { &*df_ptr };
    ctx.df.height()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_width(ptr: *mut DataFrameContext) -> usize {
    if ptr.is_null() { return 0; }
    let ctx = unsafe { &*ptr };
    ctx.df.width()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_column_name(
    df_ptr: *mut DataFrameContext, 
    index: usize
) -> *mut c_char {
    let ctx = unsafe { &*df_ptr };
    let cols = ctx.df.get_column_names();
    
    if index >= cols.len() {
        return std::ptr::null_mut();
    }

    CString::new(cols[index].as_str()).unwrap().into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_schema(df_ptr: *mut DataFrameContext) -> *mut c_char {
    let ctx = unsafe { &*df_ptr };
    let schema = ctx.df.schema();
      
    let map: std::collections::HashMap<String, String> = schema.iter_names_and_dtypes()
        .map(|(name, dtype)| (name.to_string(), dtype.to_string()))
        .collect();

    let json = serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string());
    
    CString::new(json).unwrap().into_raw()
}

// --- Convenience Ops ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_drop(df_ptr: *mut DataFrameContext, name: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let col_name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        
        let new_df = ctx.df.drop(&col_name)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_rename(df_ptr: *mut DataFrameContext, old: *const c_char, new: *const c_char) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let old_name = unsafe { CStr::from_ptr(old).to_string_lossy() };
        let new_name = unsafe { CStr::from_ptr(new).to_string_lossy() };

        // Clone + Rename
        let mut new_df = ctx.df.clone();
        new_df.rename(&old_name, PlSmallStr::from_str(&new_name))?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_drop_nulls(df_ptr: *mut DataFrameContext, subset: *const *const c_char, len: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let new_df = if subset.is_null() || len == 0 {
            ctx.df.drop_nulls::<String>(None)? 
        } else {
            let slice = unsafe { std::slice::from_raw_parts(subset, len) };
            let cols: Vec<String> = slice.iter()
                .map(|&p| unsafe { CStr::from_ptr(p).to_string_lossy().to_string() })
                .collect();
            ctx.df.drop_nulls(Some(&cols))?
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_sample_n(
    df_ptr: *mut DataFrameContext, 
    n: usize, 
    replacement: bool, 
    shuffle: bool, 
    seed: *const u64
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let s = if seed.is_null() { None } else { Some(unsafe { *seed }) };
        
        let new_df = ctx.df.sample_n_literal(n, replacement, shuffle, s)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_sample_frac(
    df_ptr: *mut DataFrameContext, 
    frac: f64, 
    replacement: bool, 
    shuffle: bool, 
    seed: *const u64
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let s = if seed.is_null() { None } else { Some(unsafe { *seed }) };
        
        let height = ctx.df.height();
        let n = (height as f64 * frac) as usize;
        
        let new_df = ctx.df.sample_n_literal(n, replacement, shuffle, s)?;
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_clone(ptr: *mut DataFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        let new_df = ctx.df.clone();
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df: new_df })))
    })
}
// --- Scalar Access ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_i64(
    df_ptr: *mut DataFrameContext, 
    col_name_ptr: *const c_char, 
    row_index: usize,
    out_val: *mut i64 
) -> bool { 
    let ctx = unsafe { &*df_ptr };
    let col_name = ptr_to_str(col_name_ptr).unwrap_or("");
    
    let col = match ctx.df.column(col_name) {
        Ok(c) => c,
        Err(_) => return false,
    };

    match col.get(row_index) {
        Ok(val) => match val {
            AnyValue::Int64(v) => { unsafe { *out_val = v }; true },
            AnyValue::Int32(v) => { unsafe { *out_val = v as i64 }; true },
            AnyValue::Int16(v) => { unsafe { *out_val = v as i64 }; true },
            AnyValue::Int8(v) =>  { unsafe { *out_val = v as i64 }; true },
            AnyValue::UInt64(v) => { 
                // i64::MAX 9,223,372,036,854,775,807
                if v > (i64::MAX as u64) {
                    // Overflow
                    // return false
                    false 
                } else {
                    unsafe { *out_val = v as i64 }; 
                    true 
                }
            },
            AnyValue::UInt32(v) => { unsafe { *out_val = v as i64 }; true }, 
            AnyValue::UInt16(v) => { unsafe { *out_val = v as i64 }; true },
            AnyValue::UInt8(v) =>  { unsafe { *out_val = v as i64 }; true },
            _ => false, 
        },
        Err(_) => false 
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_f64(
    df_ptr: *mut DataFrameContext, 
    col_name_ptr: *const c_char, 
    row_index: usize,
    out_val: *mut f64
) -> bool {
    let ctx = unsafe { &*df_ptr };
    let col_name = ptr_to_str(col_name_ptr).unwrap_or("");
    
    let col = match ctx.df.column(col_name) {
        Ok(c) => c,
        Err(_) => return false,
    };

    match col.get(row_index) {
        Ok(val) => match val {
            AnyValue::Float64(v) => { unsafe { *out_val = v }; true },
            AnyValue::Float32(v) => { unsafe { *out_val = v as f64 }; true },
            
            AnyValue::Int64(v) => { unsafe { *out_val = v as f64 }; true },
            AnyValue::Int32(v) => { unsafe { *out_val = v as f64 }; true },
            _ => false, 
        },
        Err(_) => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_string(
    df_ptr: *mut DataFrameContext, 
    col_name_ptr: *const c_char, 
    row_index: usize
) -> *mut c_char {
    let ctx = unsafe { &*df_ptr };
    let col_name = ptr_to_str(col_name_ptr).unwrap_or("");
    
    match ctx.df.column(col_name) {
        Ok(col) => match col.get(row_index) {
            Ok(AnyValue::Null) => std::ptr::null_mut(),

            Ok(AnyValue::String(s)) => CString::new(s).unwrap().into_raw(),
            Ok(AnyValue::StringOwned(s)) => CString::new(s.as_str()).unwrap().into_raw(),
            
            Ok(v) => CString::new(v.to_string()).unwrap().into_raw(),
            
            Err(_) => std::ptr::null_mut()
        },
        Err(_) => std::ptr::null_mut()
    }
}

// ==========================================
// Head/Tail
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_head(df_ptr: *mut DataFrameContext, n: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let res_df = ctx.df.head(Some(n));
        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_tail(df_ptr: *mut DataFrameContext, n: usize) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let res_df = ctx.df.tail(Some(n));
        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
// ==========================================
// Explode
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_explode(
    df_ptr: *mut DataFrameContext,
    exprs_ptr: *const *mut ExprContext,
    len: usize
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };

        if exprs.is_empty() {
             let res_df = ctx.df.clone();
             return Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })));
        }

        let mut iter = exprs.into_iter();
        
        let first_expr = iter.next().unwrap();
        let mut final_selector = first_expr.into_selector()
            .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;

        for e in iter {
            let s = e.into_selector()
                .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;
                
            final_selector = final_selector | s;
        }

        let res_df = ctx.df.clone()
            .lazy()
            .explode(final_selector)
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
// ==========================================
// Pivot & Unpivot
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_pivot(
    df_ptr: *mut DataFrameContext,
    values_ptr: *const *const c_char, values_len: usize,
    index_ptr: *const *const c_char, index_len: usize,
    columns_ptr: *const *const c_char, columns_len: usize,
    agg_code: i32
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let to_strs = |ptr, len| unsafe {
            let mut v = Vec::with_capacity(len);
            for &p in std::slice::from_raw_parts(ptr, len) {
                v.push(ptr_to_str(p).unwrap()); // 返回 &str
            }
            v
        };

        let values = to_strs(values_ptr, values_len);
        let index = to_strs(index_ptr, index_len);
        let columns = to_strs(columns_ptr, columns_len);
        
        let el = col(""); 
        let agg_expr = match agg_code {
            1 => el.sum(),    // Sum
            2 => el.min(),    // Min
            3 => el.max(),    // Max
            4 => el.mean(),   // Mean
            5 => el.median(), // Median
            6 => len(),       // Count
            7 => len(),       // Len
            8 => el.last(),   // Last
            0 | _ => el.first(), // First (Default)
        };

        let res_df = pivot_stable(
            &ctx.df,
            columns,          // I0
            Some(index),  // Option<I1>
            Some(values),   // Option<I2>
            false,          // sort_columns
            Some(agg_expr), // Option<Expr>
            None            // separator
        )?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_unpivot(
    df_ptr: *mut DataFrameContext,
    id_vars_ptr: *const *const c_char, id_len: usize,
    val_vars_ptr: *const *const c_char, val_len: usize,
    variable_name_ptr: *const c_char,
    value_name_ptr: *const c_char
) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*df_ptr };
        
        let to_pl_strs = |ptr, len| unsafe {
            let mut v = Vec::with_capacity(len);
            for &p in std::slice::from_raw_parts(ptr, len) {
                let s = ptr_to_str(p).unwrap();
                v.push(PlSmallStr::from_str(s));
            }
            v
        };

        let index_names = to_pl_strs(id_vars_ptr, id_len);
        let on_names = to_pl_strs(val_vars_ptr, val_len);

        let index_selector = cols(index_names.clone());

        let on_selector = if on_names.is_empty() {
            all().exclude_cols(index_names)
        } else {
            cols(on_names)
        };

        let variable_name = if variable_name_ptr.is_null() { None } else { Some(PlSmallStr::from_str(ptr_to_str(variable_name_ptr).unwrap())) };
        let value_name = if value_name_ptr.is_null() { None } else { Some(PlSmallStr::from_str(ptr_to_str(value_name_ptr).unwrap())) };

        let args = UnpivotArgsDSL {
            index: index_selector,
            on: on_selector,
            variable_name,
            value_name,
        };

        let res_df = ctx.df.clone()
            .lazy()
            .unpivot(args)
            .collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df: res_df })))
    })
}
// ==========================================
// Concat
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_concat(
    dfs_ptr: *const *mut DataFrameContext,
    len: usize,
    how: i32 // 0=Vertical, 1=Horizontal, 2=Diagonal
) -> *mut DataFrameContext {
    ffi_try!({
        if len == 0 {
            return Ok(Box::into_raw(Box::new(DataFrameContext { df: DataFrame::default() })));
        }

        let slice = unsafe { std::slice::from_raw_parts(dfs_ptr, len) };

        let mut dfs: Vec<DataFrame> = Vec::with_capacity(len);
        for &p in slice {
            let ctx = unsafe { Box::from_raw(p) };
            dfs.push(ctx.df);
        }

        let out_df = match how {
            0 => concat_df(&dfs)?,
            
            1 => concat_df_horizontal(&dfs,true)?,
            
            2 => concat_df_diagonal(&dfs)?,
            
            _ => return Err(PolarsError::ComputeError("Invalid concat strategy".into())),
        };

        Ok(Box::into_raw(Box::new(DataFrameContext { df: out_df })))
    })
}
// ==========================================
// Unnest
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_unnest(
    df: *mut DataFrame, 
    cols: *const *const c_char, 
    len: usize,
    separator: *const c_char
) -> *mut DataFrame {
    ffi_try!({
        let df = unsafe { &*df };
        
        let cols_slice = unsafe { std::slice::from_raw_parts(cols, len) };
        
        let names = cols_slice
            .iter()
            .map(|&ptr| unsafe { CStr::from_ptr(ptr).to_str().unwrap() });

        let sep_opt = if separator.is_null() {
            None
        } else {
            unsafe { Some(CStr::from_ptr(separator).to_str().unwrap()) }
        };

        let result_df = df.unnest(names, sep_opt)?;

        Ok(Box::into_raw(Box::new(result_df)))
    })
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_column(
    ptr: *mut DataFrameContext, 
    name: *const c_char
) -> *mut SeriesContext {
    ffi_try!({  
        let ctx = unsafe { &*ptr };
        let name_str = ptr_to_str(name).unwrap_or("");
        
        match ctx.df.column(name_str) {
            Ok(column) => {

                let s = column.as_materialized_series().clone();
                
                Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
            },
            Err(_) => Ok(std::ptr::null_mut())
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_get_column_at(
    ptr: *mut DataFrameContext, 
    index: usize
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        match ctx.df.select_at_idx(index) {
            Some(column) => {
                let s = column.as_materialized_series().clone();
                Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
            },
            None => Ok(std::ptr::null_mut())
        }
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_frame(ptr: *mut SeriesContext) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let s = ctx.series.clone();
        
        let df = DataFrame::new(vec![s.into()]).unwrap_or_default();
        
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

// Build DataFrame from Series Array
#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_new(
    columns_ptr: *const *mut SeriesContext,
    len: usize,
) -> *mut DataFrameContext {
    ffi_try!({
        if columns_ptr.is_null() || len == 0 {
            return Ok(Box::into_raw(Box::new(DataFrameContext { df: DataFrame::default() })));
        }

        let slice = unsafe { std::slice::from_raw_parts(columns_ptr, len) };
        let mut series_vec = Vec::with_capacity(len);

        for &ptr in slice {
            if !ptr.is_null() {
                let ctx = unsafe { &*ptr };
                series_vec.push(ctx.series.clone().into());
            }
        }

        let df = DataFrame::new(series_vec)?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_dataframe_new_from_stream(
    stream_ptr: *mut ArrowArrayStream,
) -> *mut DataFrameContext {
    ffi_try!({
        // 1. Check null pointer
        if stream_ptr.is_null() {
            return Err(PolarsError::ComputeError("Stream pointer is null".into()));
        }
        // 2. Raw Pointer -> Mutable Reference
        let stream = unsafe { &mut *stream_ptr};

        // 3. Create Reader
        let mut reader = unsafe {ArrowArrayStreamReader::try_new(stream)
            .map_err(|e| PolarsError::ComputeError(format!("Failed to create Arrow Stream Reader: {}", e).into()))?};

        // First Chunk
        let first_chunk_result = unsafe {reader.next()};

        // If stream is blank, return blank DataFrame
        if first_chunk_result.is_none() {
            let df = DataFrame::default();
            return Ok(Box::into_raw(Box::new(DataFrameContext { df })));
        }

        let first_chunk = first_chunk_result.unwrap()
            .map_err(|e| PolarsError::ComputeError(format!("Error reading first batch: {}", e).into()))?;

        // 3. Get Schema from first chunk
        let struct_array = first_chunk
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| PolarsError::ComputeError("First batch is not a StructArray".into()))?;

        let fields = match struct_array.dtype() {
            ArrowDataType::Struct(f) => f,
            _ => return Err(PolarsError::ComputeError("Stream data type is not Struct".into())),
        };

        let num_cols = fields.len();
        let mut columns_chunks: Vec<Vec<Box<dyn Array>>> = vec![Vec::new(); num_cols];

        // 4. Deal with first Chunk
        for (col_idx, column) in struct_array.values().iter().enumerate() {
            if col_idx < num_cols {
                columns_chunks[col_idx].push(column.clone());
            }
        }

        // 5. Deal with following chunks
        while let Some(chunk_result) = unsafe {reader.next()} {
            let chunk = chunk_result
                .map_err(|e| PolarsError::ComputeError(format!("Error reading batch: {}", e).into()))?;

            let struct_array = chunk
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| PolarsError::ComputeError("Subsequent batch is not a StructArray".into()))?;

            for (col_idx, column) in struct_array.values().iter().enumerate() {
                if col_idx < num_cols {
                    columns_chunks[col_idx].push(column.clone());
                }
            }
        }

        // 6. Build Series
        let mut series_vec = Vec::with_capacity(num_cols);

        for (i, chunks) in columns_chunks.into_iter().enumerate() {
            let arrow_field = &fields[i]; // &ArrowField
            
            let p_field = PolarsField::from(arrow_field); 
            let p_dtype = p_field.dtype;
            let name = p_field.name.as_str();

            let s = unsafe {Series::from_chunks_and_dtype_unchecked(name.into(), chunks, &p_dtype)};
            
            series_vec.push(s.into());
        }

        // 7. Return DataFrame
        let df = DataFrame::new(series_vec)?;
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_lazy(df_ptr: *mut DataFrameContext) -> *mut LazyFrameContext {
    let ctx = unsafe { &*df_ptr };
    let inner = ctx.df.clone().lazy();
    
    Box::into_raw(Box::new(LazyFrameContext { inner }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_dataframe_to_string(df_ptr: *mut DataFrameContext) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &mut *df_ptr };
        let s = ctx.df.to_string();
        
        let c_str = CString::new(s).unwrap();
        Ok(c_str.into_raw())
    })
}