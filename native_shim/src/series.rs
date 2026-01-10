use polars::prelude::*;
use polars_arrow::array::{Array, ListArray};
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use crate::types::{DataFrameContext, DataTypeContext, SeriesContext};
use crate::utils::*;

// ==========================================
// Constructors 
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_i32(
    name: *const c_char, 
    ptr: *const i32, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        
        let series = if validity.is_null() {
            Series::new(name.into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<i32>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.into(), &opts)
        };

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_i64(
    name: *const c_char, 
    ptr: *const i64, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };

        let series = if validity.is_null() {
            Series::new(name.into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<i64>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.into(), &opts)
        };

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_f64(
    name: *const c_char, 
    ptr: *const f64, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };

        let series = if validity.is_null() {
            Series::new(name.into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<f64>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.into(), &opts)
        };

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_bool(
    name: *const c_char, 
    ptr: *const bool, 
    validity: *const bool, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };

        let series = if validity.is_null() {
            Series::new(name.into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<bool>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.into(), &opts)
        };

        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_str(
    name: *const c_char, 
    strs: *const *const c_char, 
    len: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        let slice = unsafe { std::slice::from_raw_parts(strs, len) };
        
        let vec_opts: Vec<Option<&str>> = slice.iter()
            .map(|&p| {
                if p.is_null() {
                    None 
                } else {
                    unsafe { Some(CStr::from_ptr(p).to_str().unwrap_or("")) }
                }
            })
            .collect();

        let series = Series::new(name.into(), &vec_opts);
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_new_decimal(
    name: *const c_char,
    ptr: *const i128,
    validity: *const bool,
    len: usize,
    scale: usize
) -> *mut SeriesContext {
    ffi_try!({
        let name = unsafe { CStr::from_ptr(name).to_string_lossy() };
        
        let slice = unsafe { std::slice::from_raw_parts(ptr, len) };
        let series = if validity.is_null() {
            Series::new(name.clone().into(), slice)
        } else {
            let v_slice = unsafe { std::slice::from_raw_parts(validity, len) };
            let opts: Vec<Option<i128>> = slice.iter().zip(v_slice.iter())
                .map(|(&v, &valid)| if valid { Some(v) } else { None })
                .collect();
            Series::new(name.clone().into(), &opts)
        };

        let decimal_series = series
            .i128()
            .map_err(|_| PolarsError::ComputeError("Failed to cast to i128 for decimal creation".into()))?
            .clone()
            .into_decimal(None, scale)
            .map_err(|e| PolarsError::ComputeError(format!("Decimal creation failed: {}", e).into()))?
            .into_series();

        Ok(Box::into_raw(Box::new(SeriesContext { series: decimal_series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_clone(ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        
        let new_series = ctx.series.clone();
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: new_series })))
    })
}
// ==========================================
// Methods
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_free(ptr: *mut SeriesContext) {
    if !ptr.is_null() {
        unsafe { let _ = Box::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_len(ptr: *mut SeriesContext) -> usize {
    let ctx = unsafe { &*ptr };
    ctx.series.len()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_name(ptr: *mut SeriesContext) -> *mut c_char {
    let ctx = unsafe { &*ptr };
    CString::new(ctx.series.name().as_str()).unwrap().into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_rename(ptr: *mut SeriesContext, name: *const c_char) {
    let ctx = unsafe { &mut *ptr };
    let name_str = unsafe { CStr::from_ptr(name).to_string_lossy() };
    ctx.series.rename(name_str.into());
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_string(s_ptr: *mut SeriesContext) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let s = std::string::ToString::to_string(&ctx.series); // Native Display
        let c_str = CString::new(s).unwrap();
        Ok(c_str.into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_slice(series: *mut Series, offset: i64, length: usize) -> *mut Series {
    let s = unsafe { &*series };
    let new_s = s.slice(offset, length);
    Box::into_raw(Box::new(new_s))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_dtype_str(s_ptr: *mut SeriesContext) -> *mut c_char {
    let ctx = unsafe { &*s_ptr };
    let dtype_str = ctx.series.dtype().to_string();
    CString::new(dtype_str).unwrap().into_raw()
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_to_arrow(ptr: *mut SeriesContext) -> *mut ArrowArrayContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let contiguous_series = ctx.series.rechunk();
        let arr = contiguous_series.to_arrow(0, CompatLevel::newest());
        Ok(Box::into_raw(Box::new(ArrowArrayContext { array: arr })))
    })
}

pub fn upgrade_to_large_list(array: Box<dyn Array>) -> Box<dyn Array> {
    match array.dtype() {
        ArrowDataType::List(inner_field) => {
            // Convert to ListArray<i32>
            let list_array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();

            // Convert Offsets (i32 -> i64)
            let offsets_i32 = list_array.offsets();
            let offsets_i64: Vec<i64> = offsets_i32.iter().map(|&x| x as i64).collect();
            
            // Convert Arrow Buffer
            let raw_buffer = polars_arrow::buffer::Buffer::from(offsets_i64);
            let offsets_buffer = polars_arrow::offset::OffsetsBuffer::try_from(raw_buffer).unwrap();

            // Deal Values Recursively
            let values = list_array.values().clone();
            let new_values = upgrade_to_large_list(values);

            // Build new DataType (LargeList)
            let new_inner_dtype = new_values.dtype().clone();
            let new_field = inner_field.as_ref().clone().with_dtype(new_inner_dtype);
            let new_dtype = ArrowDataType::LargeList(Box::new(new_field));

            // Build New LargeListArray
            // new(data_type, offsets, values, validity)
            let large_list = ListArray::<i64>::new(
                new_dtype,
                offsets_buffer.into(),
                new_values,
                list_array.validity().cloned(),
            );

            Box::new(large_list)
        },
        
        ArrowDataType::LargeList(inner_field) => {
             let list_array = array.as_any().downcast_ref::<ListArray<i64>>().unwrap();
             
             let values = list_array.values().clone();
             let new_values = upgrade_to_large_list(values.clone());
             
             if new_values.dtype() == values.dtype() {
                 return array;
             }

             let new_inner_dtype = new_values.dtype().clone();
             let new_field = inner_field.as_ref().clone().with_dtype(new_inner_dtype);
             let new_dtype = ArrowDataType::LargeList(Box::new(new_field));
             
             let large_list = ListArray::<i64>::new(
                new_dtype,
                list_array.offsets().clone(),
                new_values,
                list_array.validity().cloned(),
            );
            Box::new(large_list)
        },
        ArrowDataType::Struct(fields) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            
            let new_values: Vec<Box<dyn Array>> = struct_array
                .values()
                .iter()
                .map(|v| upgrade_to_large_list(v.clone())) 
                .collect();

            let mut changed = false;
            for (old, new) in struct_array.values().iter().zip(new_values.iter()) {
                if old.dtype() != new.dtype() {
                    changed = true;
                    break;
                }
            }

            if !changed {
                return array;
            }

            let new_fields: Vec<ArrowField> = fields
                .iter()
                .zip(new_values.iter())
                .map(|(f, v)| {
                    f.clone().with_dtype(v.dtype().clone())
                })
                .collect();
            
            let new_dtype = ArrowDataType::Struct(new_fields);

            let new_struct = StructArray::new(
                new_dtype,
                struct_array.len(),
                new_values,
                struct_array.validity().cloned(),
            );

            Box::new(new_struct)
        },
        _ => array,
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_arrow_to_series(
    name: *const c_char,
    ptr_array: *mut polars_arrow::ffi::ArrowArray,
    ptr_schema: *mut polars_arrow::ffi::ArrowSchema
) -> *mut SeriesContext {
    ffi_try!({
        let name_str = unsafe { CStr::from_ptr(name).to_str().unwrap() };
        let field = unsafe { polars_arrow::ffi::import_field_from_c(&*ptr_schema)? };

        let array_val = unsafe { std::ptr::read(ptr_array) };
        let mut array = unsafe { polars_arrow::ffi::import_array_from_c(array_val, field.dtype)? };
       
        array = upgrade_to_large_list(array);

        let series = Series::from_arrow(name_str.into(), array)?;
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_cast(
    ptr: *mut SeriesContext, 
    dtype_ptr: *mut DataTypeContext
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*ptr };
        let target_dtype = unsafe { &(*dtype_ptr).dtype };
        
        let s = ctx.series.cast(target_dtype)?;
        Ok(Box::into_raw(Box::new(SeriesContext { series: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_null(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let series = ctx.series.is_null().into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_not_null(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let series = ctx.series.is_not_null().into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_null_at(s_ptr: *mut SeriesContext, idx: usize) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { 
        return false; 
    }
    match ctx.series.get(idx) {
        Ok(AnyValue::Null) => true,
        _ => false 
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_null_count(s_ptr: *mut SeriesContext) -> usize {
    let ctx = unsafe { &*s_ptr };
    ctx.series.null_count()
}

// Unique
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_unique(ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &*ptr }.series.clone();
        let out = s.unique()?;
        Ok(Box::into_raw(Box::new(SeriesContext { series: out })))
    })
}

// UniqueStable
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_unique_stable(ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &*ptr }.series.clone();
        let out = s.unique_stable()?;
        Ok(Box::into_raw(Box::new(SeriesContext { series: out })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_n_unique(ptr: *mut SeriesContext) -> usize {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if ptr.is_null() {
            return 0;
        }
        let ctx = unsafe { &*ptr };
        ctx.series.n_unique().unwrap_or(0)
    }));

    result.unwrap_or(0)
}
// --- Scalar Access ---

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_i64(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }

    match ctx.series.get(idx) {
        Ok(AnyValue::Int64(v)) => { unsafe { *out_val = v }; true }
        Ok(AnyValue::Int32(v)) => { unsafe { *out_val = v as i64 }; true }
        Ok(AnyValue::Int16(v)) => { unsafe { *out_val = v as i64 }; true }
        Ok(AnyValue::Int8(v)) => { unsafe { *out_val = v as i64 }; true }
        Ok(AnyValue::UInt64(v)) => { unsafe { *out_val = v as i64 }; true } 
        Ok(AnyValue::UInt32(v)) => { unsafe { *out_val = v as i64 }; true }
        _ => false // Null or type mismatch
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_f64(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut f64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }

    match ctx.series.get(idx) {
        Ok(AnyValue::Float64(v)) => { unsafe { *out_val = v }; true }
        Ok(AnyValue::Float32(v)) => { unsafe { *out_val = v as f64 }; true }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_bool(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut bool) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }

    match ctx.series.get(idx) {
        Ok(AnyValue::Boolean(v)) => { unsafe { *out_val = v }; true }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_str(s_ptr: *mut SeriesContext, idx: usize) -> *mut c_char {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return std::ptr::null_mut(); }

    match ctx.series.get(idx) {
        Ok(AnyValue::String(s)) => CString::new(s).unwrap().into_raw(),
        _ => std::ptr::null_mut()
    }
}

// Decimal 
// out_val: i128 value
// out_scale: scale 
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_decimal(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i128, out_scale: *mut usize) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }

    match ctx.series.get(idx) {
        Ok(AnyValue::Decimal(v, scale)) => { 
            unsafe { 
                *out_val = v; 
                *out_scale = scale;
            } 
            true 
        }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_date(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i32) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }
    match ctx.series.get(idx) {
        Ok(AnyValue::Date(v)) => { unsafe { *out_val = v }; true }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_time(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }
    match ctx.series.get(idx) {
        Ok(AnyValue::Time(v)) => { unsafe { *out_val = v }; true } // Nanoseconds
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_datetime(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }
    match ctx.series.get(idx) {
        // Datetime(val, unit, timezone)
        Ok(AnyValue::Datetime(v, _, _)) => { unsafe { *out_val = v }; true }
        _ => false
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_get_duration(s_ptr: *mut SeriesContext, idx: usize, out_val: *mut i64) -> bool {
    let ctx = unsafe { &*s_ptr };
    if idx >= ctx.series.len() { return false; }
    match ctx.series.get(idx) {
        Ok(AnyValue::Duration(v, _)) => { unsafe { *out_val = v }; true }
        _ => false
    }
}

// ==========================================
// Arithmetic Ops 
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_add(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1 + s2; 
        Ok(Box::into_raw(Box::new(SeriesContext { series: res? })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_sub(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1 - s2;
        Ok(Box::into_raw(Box::new(SeriesContext { series: res? })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_mul(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1 * s2;
        Ok(Box::into_raw(Box::new(SeriesContext { series: res? })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_div(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1 / s2;
        Ok(Box::into_raw(Box::new(SeriesContext { series: res? })))
    })
}

// ==========================================
// Comparison Ops 
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_eq(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.equal(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_neq(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.not_equal(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_gt(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.gt(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_gt_eq(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.gt_eq(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_lt(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.lt(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_lt_eq(s1: *mut SeriesContext, s2: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s1 = unsafe { &(*s1).series };
        let s2 = unsafe { &(*s2).series };
        let res = s1.lt_eq(s2).map_err(|e| PolarsError::ComputeError(e.to_string().into()))?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}
// ==========================================
// Aggregations
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_sum(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        let res = s.sum_reduce()?.into_series(s.name().clone());
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_mean(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        let mean_val = s.mean();
        let res = Series::new(s.name().clone(), &[mean_val]);
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_min(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        
        let scalar = s.min_reduce()?;
        
        let res = scalar.into_series(s.name().clone());
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_max(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let s = unsafe { &(*s_ptr).series };
        
        let scalar = s.max_reduce()?;
        
        let res = scalar.into_series(s.name().clone());
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_nan(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let res = ctx.series.is_nan()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_not_nan(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let res = ctx.series.is_not_nan()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_finite(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let res = ctx.series.is_finite()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_is_infinite(s_ptr: *mut SeriesContext) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*s_ptr };
        let res = ctx.series.is_infinite()?.into_series();
        Ok(Box::into_raw(Box::new(SeriesContext { series: res })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_series_get_dtype(ptr: *mut Series) -> *mut DataType {
    ffi_try!({
        let s = unsafe {&*ptr};
        Ok(Box::into_raw(Box::new(s.dtype().clone())))
    })
}

// ==========================================
// Operations
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_series_sort(
    series_ptr: *mut SeriesContext,
    descending: bool,
    nulls_last: bool,
    multithreaded: bool,
    maintain_order: bool
) -> *mut SeriesContext {
    ffi_try!({
        let ctx = unsafe { &*series_ptr };
        
        let options = SortOptions {
            descending,
            nulls_last,
            multithreaded,
            maintain_order,
            limit: None, 
        };

        let out = ctx.series.sort(options)?;
        
        Ok(Box::into_raw(Box::new(SeriesContext { series: out })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_series_struct_unnest(series_ptr: *mut SeriesContext) -> *mut DataFrameContext {
    ffi_try!({
        let ctx = unsafe { &*series_ptr };
        let s = &ctx.series;

        let ca = s.struct_()?;

        let df = ca.clone().unnest();

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}