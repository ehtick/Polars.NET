use polars::prelude::*;
use std::os::raw::c_char;
use crate::{types::{ExprContext, SelectorContext}, utils::ptr_to_str};

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_free(ptr: *mut SelectorContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

// =================================================================
// Basic Selectors
// =================================================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_all() -> *mut SelectorContext {
    ffi_try!({
        let s = all();
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// selector.exclude(["a", "b"])
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_exclude(
    sel_ptr: *mut SelectorContext,
    names_ptr: *const *const c_char,
    len: usize
) -> *mut SelectorContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(sel_ptr) };
        
        let mut exclusions = Vec::with_capacity(len);
        let slice = unsafe { std::slice::from_raw_parts(names_ptr, len) };
        for &p in slice {
            let s = ptr_to_str(p).unwrap();
            exclusions.push(PlSmallStr::from_str(s));
        }

        let new_sel = ctx.inner.exclude_cols(exclusions);
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: new_sel })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_cols(
    names_ptr: *const *const c_char,
    len: usize
) -> *mut SelectorContext {
    ffi_try!({
        // Build Vec<PlSmallStr>
        let mut names_vec = Vec::with_capacity(len);
        if len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(names_ptr, len) };
            for &p in slice {
                let s = ptr_to_str(p).unwrap();
                names_vec.push(PlSmallStr::from_string(s.to_string()));
            }
        }

        // Build Selector::ByName
        // Convert Vec to Arc<[T]>
        let names_arc: Arc<[PlSmallStr]> = names_vec.into();

        let s = Selector::ByName {
            names: names_arc,
            strict: true, 
        };
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}
// =================================================================
// String Matchers (StartsWith, EndsWith, Contains, Regex)
// =================================================================

fn to_small_str(ptr: *const c_char) -> PolarsResult<PlSmallStr> {
    let s = ptr_to_str(ptr).unwrap();
    Ok(PlSmallStr::from_str(s))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_starts_with(
    pattern: *const c_char
) -> *mut SelectorContext {
    ffi_try!({
        let p = ptr_to_str(pattern).unwrap();
        let regex = format!("^{}", p);
        let s = Selector::Matches(PlSmallStr::from_str(&regex));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_ends_with(
    pattern: *const c_char
) -> *mut SelectorContext {
    ffi_try!({
        let p = ptr_to_str(pattern).unwrap();
        let regex = format!("{}$", p);
        let s = Selector::Matches(PlSmallStr::from_str(&regex));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_contains(
    pattern: *const c_char
) -> *mut SelectorContext {
    ffi_try!({
        let p = ptr_to_str(pattern).unwrap();
        let s = Selector::Matches(PlSmallStr::from_str(p));
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_match(
    pattern: *const c_char
) -> *mut SelectorContext {
    ffi_try!({
        let p = to_small_str(pattern).unwrap();
        let s = Selector::Matches(p);
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// =================================================================
// Type Selectors (DataTypeSelector)
// =================================================================

// Helper：Wrap DataType to DataTypeSelector::AnyOf
#[inline]
fn dt_selector_single(dt: DataType) -> DataTypeSelector {
    DataTypeSelector::AnyOf(Arc::new([dt]))
}

// C# PlDataType (i32) -> Rust DataTypeSelector
fn map_i32_to_dtype_selector(kind: i32) -> DataTypeSelector {
    match kind {
        // 0: Unknown / SameAsInput
        0 => DataTypeSelector::Empty,

        // 1: Boolean
        1 => dt_selector_single(DataType::Boolean),

        // Integers
        2 => dt_selector_single(DataType::Int8),
        3 => dt_selector_single(DataType::Int16),
        4 => dt_selector_single(DataType::Int32),
        5 => dt_selector_single(DataType::Int64),

        // Unsigned Integers
        6 => dt_selector_single(DataType::UInt8),
        7 => dt_selector_single(DataType::UInt16),
        8 => dt_selector_single(DataType::UInt32),
        9 => dt_selector_single(DataType::UInt64),

        // Floats
        10 => dt_selector_single(DataType::Float32),
        11 => dt_selector_single(DataType::Float64),

        // String
        12 => dt_selector_single(DataType::String),

        // Temporal - Date
        13 => dt_selector_single(DataType::Date),

        // Temporal - Datetime
        14 => DataTypeSelector::Datetime(TimeUnitSet::all(), TimeZoneSet::Any),

        // Temporal - Time
        15 => dt_selector_single(DataType::Time),

        // Temporal - Duration
        16 => DataTypeSelector::Duration(TimeUnitSet::all()),

        // Binary
        17 => dt_selector_single(DataType::Binary),

        // Null 
        18 => dt_selector_single(DataType::Null),

        // Struct
        19 => DataTypeSelector::Struct,

        20 => DataTypeSelector::List(None),

        21 => DataTypeSelector::Categorical,
        22 => DataTypeSelector::Decimal,
        23 => DataTypeSelector::Array(None, None),
        24 => dt_selector_single(DataType::Int128),
        25 => dt_selector_single(DataType::UInt128),
        26 => dt_selector_single(DataType::Float16),
        // Empty
        _ => DataTypeSelector::Empty,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_by_dtype(kind: i32) -> *mut SelectorContext {
    ffi_try!({
        let dts = map_i32_to_dtype_selector(kind);
        // Selector::ByDType(DataTypeSelector)
        let s = Selector::ByDType(dts);
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_numeric() -> *mut SelectorContext {
    ffi_try!({
        // DataTypeSelector::Numeric
        let s = Selector::ByDType(DataTypeSelector::Numeric);
        Ok(Box::into_raw(Box::new(SelectorContext { inner: s })))
    })
}

// =================================================================
// Set Operations
// =================================================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_and(
    left: *mut SelectorContext,
    right: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let l = unsafe { Box::from_raw(left) };
        let r = unsafe { Box::from_raw(right) };
        
        // Selector::Intersect(lhs, rhs)
        let res = Selector::Intersect(Arc::new(l.inner), Arc::new(r.inner));
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_or(
    left: *mut SelectorContext,
    right: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let l = unsafe { Box::from_raw(left) };
        let r = unsafe { Box::from_raw(right) };
        
        // Selector::Union(lhs, rhs)
        let res = Selector::Union(Arc::new(l.inner), Arc::new(r.inner));
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: res })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_not(
    ptr: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let l = unsafe { Box::from_raw(ptr) };
        
        let res = Selector::Difference(
            Arc::new(Selector::Wildcard),
            Arc::new(l.inner)
        );
        
        Ok(Box::into_raw(Box::new(SelectorContext { inner: res })))
    })
}

// =================================================================
// Bridges
// =================================================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_into_expr(
    sel_ptr: *mut SelectorContext
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(sel_ptr) };
        let expr: Expr = ctx.inner.into(); 
        Ok(Box::into_raw(Box::new(ExprContext { inner: expr })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_selector_clone(
    sel_ptr: *mut SelectorContext
) -> *mut SelectorContext {
    ffi_try!({
        let ctx = unsafe { &*sel_ptr };
        let new_sel = ctx.inner.clone();
        Ok(Box::into_raw(Box::new(SelectorContext { inner: new_sel })))
    })
}