use std::any::Any;
use std::ffi::{CStr, c_char};
use polars::prelude::*;
use crate::types::*;
use polars::lazy::dsl::UnpivotArgsDSL;
use crate::utils::{consume_exprs_array, map_jointype, ptr_to_str};

// ==========================================
// Macro Definition
// ==========================================

/// LazyFrame -> Vec<Expr> -> LazyFrame
/// for select, with_columns
macro_rules! gen_lazy_vec_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext,
            exprs_ptr: *const *mut ExprContext,
            len: usize
        ) -> *mut LazyFrameContext {
            ffi_try!({
                // Consume LazyFrame handle
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                
                // Take back the ownership of Exprs
                let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };

                // Transform
                let new_lf = lf_ctx.inner.$method(exprs);

                // Return new context
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

/// LazyFrame -> Single Expr -> LazyFrame
/// for: filter
macro_rules! gen_lazy_single_expr_op {
    ($func_name:ident, $method:ident) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext, 
            expr_ptr: *mut ExprContext
        ) -> *mut LazyFrameContext {
            ffi_try!({
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                let expr_ctx = unsafe { Box::from_raw(expr_ptr) };
                
                let new_lf = lf_ctx.inner.$method(expr_ctx.inner);
                
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

/// LazyFrame -> Scalar Parameters -> LazyFrame
/// For: limit (u32), head (u32)
macro_rules! gen_lazy_scalar_op {
    ($func_name:ident, $method:ident, $arg_type:ty) => {
        #[unsafe(no_mangle)]
        pub extern "C" fn $func_name(
            lf_ptr: *mut LazyFrameContext, 
            val: $arg_type
        ) -> *mut LazyFrameContext {
            ffi_try!({
                let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
                let new_lf = lf_ctx.inner.$method(val); 
                Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
            })
        }
    };
}

// ==========================================
// Macro (Standard API)
// ==========================================

// --- Select / WithColumns ---
gen_lazy_vec_op!(pl_lazy_select, select);
gen_lazy_vec_op!(pl_lazy_with_columns, with_columns);

// --- Filter ---
gen_lazy_single_expr_op!(pl_lazy_filter, filter);

// --- Limit ---
gen_lazy_scalar_op!(pl_lazy_limit, limit, u32);
gen_lazy_scalar_op!(pl_lazy_tail, tail, u32);

// ==========================================
// Sort
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_sort(
    lf_ptr: *mut LazyFrameContext,
    expr_ptrs: *const *mut ExprContext,
    expr_len: usize,
    descending_ptr: *const bool,
    descending_len: usize,
    nulls_last_ptr: *const bool,   
    nulls_last_len: usize,         
    maintain_order: bool           
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        
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

        let res_lf = ctx.inner.sort_by_exprs(exprs, options);

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: res_lf })))
    })
}

fn build_sort_options(descending: Vec<bool>) -> SortMultipleOptions {
    let len = descending.len();
    SortMultipleOptions {
        descending,
        nulls_last: vec![true; len], 
        multithreaded: true,
        maintain_order: false,
        limit:None
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_top_k(
    lf_ptr: *mut LazyFrameContext,
    k: u32,
    by_ptrs: *const *mut ExprContext,
    by_len: usize,
    reverse_ptr: *const bool, 
    reverse_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let mut by_exprs = Vec::with_capacity(by_len);
        if by_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(by_ptrs, by_len) };
            for &p in slice {
                let e = unsafe { Box::from_raw(p) };
                by_exprs.push(e.inner);
            }
        }

        let mut reverse = Vec::with_capacity(reverse_len);
        if reverse_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(reverse_ptr, reverse_len) };
            reverse.extend_from_slice(slice);
        }

        let options = build_sort_options(reverse);

        let new_lf = ctx.inner.top_k(k, by_exprs, options);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_bottom_k(
    lf_ptr: *mut LazyFrameContext,
    k: u32,
    by_ptrs: *const *mut ExprContext,
    by_len: usize,
    reverse_ptr: *const bool,
    reverse_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let mut by_exprs = Vec::with_capacity(by_len);
        if by_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(by_ptrs, by_len) };
            for &p in slice {
                let e = unsafe { Box::from_raw(p) };
                by_exprs.push(e.inner);
            }
        }

        let mut reverse = Vec::with_capacity(reverse_len);
        if reverse_len > 0 {
            let slice = unsafe { std::slice::from_raw_parts(reverse_ptr, reverse_len) };
            reverse.extend_from_slice(slice);
        }

        let options = build_sort_options(reverse);

        let new_lf = ctx.inner.bottom_k(k, by_exprs, options);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// GroupBy
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_groupby_agg(
    lf_ptr: *mut LazyFrameContext,
    keys_ptr: *const *mut ExprContext, keys_len: usize,
    aggs_ptr: *const *mut ExprContext, aggs_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let keys = unsafe { consume_exprs_array(keys_ptr, keys_len) };
        let aggs = unsafe { consume_exprs_array(aggs_ptr, aggs_len) };

        let new_lf = lf_ctx.inner.group_by_stable(keys).agg(aggs);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazy_group_by_dynamic(
    lf_ptr: *mut LazyFrameContext,
    index_col: *const c_char,
    every: *const c_char,
    period: *const c_char,
    offset: *const c_char,
    label_idx: i32,         
    include_boundaries: bool,
    closed_window_idx: i32, 
    start_by_idx: i32,      
    // --- Keys & Aggs ---
    keys_ptr: *const *mut ExprContext, keys_len: usize,
    aggs_ptr: *const *mut ExprContext, aggs_len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let index_col_str = unsafe { CStr::from_ptr(index_col).to_str().unwrap() };
        let every_str = unsafe { CStr::from_ptr(every).to_str().unwrap() };
        let period_str = unsafe { CStr::from_ptr(period).to_str().unwrap() };
        let offset_str = unsafe { CStr::from_ptr(offset).to_str().unwrap() };

        let closed_window = match closed_window_idx {
            0 => ClosedWindow::Left,
            1 => ClosedWindow::Right,
            2 => ClosedWindow::Both,
            3 => ClosedWindow::None,
            _ => ClosedWindow::Left,
        };

        let label = match label_idx {
            0 => Label::Left,
            1 => Label::Right,
            2 => Label::DataPoint,
            _ => Label::Left,
        };

        let start_by = match start_by_idx {
            0 => StartBy::WindowBound,
            1 => StartBy::DataPoint,
            2 => StartBy::Monday,
            3 => StartBy::Tuesday,
            4 => StartBy::Wednesday,
            5 => StartBy::Thursday,
            6 => StartBy::Friday,
            7 => StartBy::Saturday,
            8 => StartBy::Sunday,
            _ => StartBy::WindowBound,
        };

        let options = DynamicGroupOptions {
            index_column: PlSmallStr::from_str(index_col_str), // 初始化一下
            every: Duration::parse(every_str),
            period: Duration::parse(period_str),
            offset: Duration::parse(offset_str),
            label,    
            include_boundaries,
            closed_window,
            start_by,
        };

        let keys = unsafe { consume_exprs_array(keys_ptr, keys_len) };
        let aggs = unsafe { consume_exprs_array(aggs_ptr, aggs_len) };

        // group_by_dynamic(self, index_column: Expr, group_by: E, options: DynamicGroupOptions)
        let new_lf = lf_ctx.inner
            .group_by_dynamic(
                col(index_col_str), 
                keys, 
                options
            )
            .agg(aggs);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_explode(
    lf_ptr: *mut LazyFrameContext,
    exprs_ptr: *const *mut ExprContext,
    len: usize
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let exprs = unsafe { consume_exprs_array(exprs_ptr, len) };

        if exprs.is_empty() {
            return Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf_ctx.inner })));
        }

        let mut iter = exprs.into_iter();
        
        let first_expr = iter.next().unwrap();
        let mut final_selector = first_expr.into_selector()
            .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;

        for e in iter {
            let s = e.into_selector()
                .ok_or_else(|| PolarsError::ComputeError("Expr cannot be converted to Selector".into()))?;
            
            final_selector = final_selector | s; // Union
        }

        let new_lf = lf_ctx.inner.explode(final_selector);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazyframe_unnest(
    lf_ptr: *mut LazyFrameContext,
    sel_ptr: *mut SelectorContext,
    separator_ptr: *const c_char 
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        let sel_ctx = unsafe {  Box::from_raw(sel_ptr) }; 

        let sep = if separator_ptr.is_null() {
            None
        } else {
            let s = ptr_to_str(separator_ptr).unwrap();
            Some(PlSmallStr::from_str(s))
        };

        let new_lf = lf_ctx.inner.unnest(sel_ctx.inner, sep);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

// ==========================================
// Collect (LazyFrame -> DataFrame)
// ==========================================

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_collect(lf_ptr: *mut LazyFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        let df = lf_ctx.inner.collect()?;

        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_collect_streaming(lf_ptr: *mut LazyFrameContext) -> *mut DataFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
        // Polars 0.50+ API: with_streaming(true).collect()
        let df = lf_ctx.inner
            .with_new_streaming(true)
            .collect()?;
            
        Ok(Box::into_raw(Box::new(DataFrameContext { df })))
    })
}
// ==========================================
// Unpivot
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_unpivot(
    lf_ptr: *mut LazyFrameContext,
    id_vars_ptr: *const *const c_char, id_len: usize,
    val_vars_ptr: *const *const c_char, val_len: usize,
    variable_name_ptr: *const c_char,
    value_name_ptr: *const c_char
) -> *mut LazyFrameContext {
    ffi_try!({
        let lf_ctx = unsafe { Box::from_raw(lf_ptr) };
        
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

        let variable_name = if variable_name_ptr.is_null() { 
            None 
        } else { 
            Some(PlSmallStr::from_str(ptr_to_str(variable_name_ptr).unwrap())) 
        };
        
        let value_name = if value_name_ptr.is_null() { 
            None 
        } else { 
            Some(PlSmallStr::from_str(ptr_to_str(value_name_ptr).unwrap())) 
        };

        let args = UnpivotArgsDSL {
            index: index_selector, 
            on: on_selector,       
            variable_name,
            value_name,
        };

        let new_lf = lf_ctx.inner.unpivot(args);
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// Concat
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_concat(
    lfs_ptr: *const *mut LazyFrameContext, 
    len: usize,
    how: i32,        // 0=Vert, 1=Horz, 2=Diag
    rechunk: bool,   
    parallel: bool   
) -> *mut LazyFrameContext {
    ffi_try!({
        let mut lfs = Vec::with_capacity(len);
        let slice = unsafe { std::slice::from_raw_parts(lfs_ptr, len) };
        
        for &p in slice {
            let lf_ctx = unsafe { Box::from_raw(p) };
            lfs.push(lf_ctx.inner);
        }

        if lfs.is_empty() {
             return Err(PolarsError::ComputeError("Cannot concat empty list of LazyFrames".into()));
        }

        let args = UnionArgs {
            rechunk,
            parallel,
            ..Default::default()
        };

        let new_lf = match how {
            // Vertical
            0 => concat(lfs, args)?,
            
            // Horizontal
            1 => concat_lf_horizontal(lfs, args)?,

            // Diagonal
            2 => concat_lf_diagonal(lfs, args)?,

            _ => return Err(PolarsError::ComputeError("Invalid lazy concat strategy".into())),
        };
        
        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}

// ==========================================
// Join & Join As of
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_join(
    left_ptr: *mut LazyFrameContext,
    right_ptr: *mut LazyFrameContext,
    left_on_ptr: *const *mut ExprContext, left_on_len: usize,
    right_on_ptr: *const *mut ExprContext, right_on_len: usize,
    how_code: i32 
) -> *mut LazyFrameContext {
    ffi_try!({
        let left_ctx = unsafe { Box::from_raw(left_ptr) };
        let right_ctx = unsafe { Box::from_raw(right_ptr) };

        let left_on = unsafe { consume_exprs_array(left_on_ptr, left_on_len) };
        let right_on = unsafe { consume_exprs_array(right_on_ptr, right_on_len) };

        let how = map_jointype(how_code);
        let args = JoinArgs::new(how);

        let new_lf = left_ctx.inner.join(right_ctx.inner, left_on, right_on, args);

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
fn exprs_to_names(exprs: &[Expr]) -> PolarsResult<Vec<PlSmallStr>> {
    let mut names = Vec::new();
    for e in exprs {
        // e.meta().root_names() returns Vec<PlSmallStr>
        let roots = e.clone().meta().root_names();
        
        names.extend_from_slice(&roots);
    }
    Ok(names)
}
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_join_asof(
    left_ptr: *mut LazyFrameContext,
    right_ptr: *mut LazyFrameContext,
    left_on_ptr: *mut ExprContext,
    right_on_ptr: *mut ExprContext,
    by_left_ptr: *const *mut ExprContext, by_left_len: usize,
    by_right_ptr: *const *mut ExprContext, by_right_len: usize,
    strategy_ptr: *const c_char,
    tolerance_ptr: *const c_char 
) -> *mut LazyFrameContext {
    ffi_try!({
        let left = unsafe { Box::from_raw(left_ptr) };
        let right = unsafe { Box::from_raw(right_ptr) };
        let left_on = unsafe { Box::from_raw(left_on_ptr) };
        let right_on = unsafe { Box::from_raw(right_on_ptr) };
        
        let by_left_exprs = unsafe { consume_exprs_array(by_left_ptr, by_left_len) };
        let by_right_exprs = unsafe { consume_exprs_array(by_right_ptr, by_right_len) };

        let left_by_names = if by_left_exprs.is_empty() { None } else { Some(exprs_to_names(&by_left_exprs)?) };
        let right_by_names = if by_right_exprs.is_empty() { None } else { Some(exprs_to_names(&by_right_exprs)?) };

        let strategy_str = ptr_to_str(strategy_ptr).unwrap_or("backward");
        let strategy = match strategy_str {
            "forward" => AsofStrategy::Forward,
            "nearest" => AsofStrategy::Nearest,
            _ => AsofStrategy::Backward,
        };

        let tol_str = if tolerance_ptr.is_null() { "" } else { ptr_to_str(tolerance_ptr).unwrap() };
        
        let (tolerance, tolerance_str_val) = if tol_str.is_empty() {
            (None, None)
        } else if let Ok(v) = tol_str.parse::<i64>() {
            // Pure Interger -> Scalar(Int64)
            (Some(Scalar::new(DataType::Int64, AnyValue::Int64(v))), None)
        } else if let Ok(v) = tol_str.parse::<f64>() {
            // Float -> Scalar(Float64)
            (Some(Scalar::new(DataType::Float64, AnyValue::Float64(v))), None)
        } else {
            (None, Some(PlSmallStr::from_str(tol_str)))
        };

        // Build Options
        let options = AsOfOptions {
            strategy,
            tolerance,      // Option<Scalar>
            tolerance_str: tolerance_str_val, // Option<PlSmallStr>
            left_by: left_by_names,
            right_by: right_by_names,
            allow_eq: true, 
            check_sortedness: true, 
        };

        let new_lf = left.inner.join_builder()
            .with(right.inner)
            .left_on([left_on.inner])
            .right_on([right_on.inner])
            .how(JoinType::AsOf(Box::new(options)))
            .finish();

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: new_lf })))
    })
}
// ==========================================
// Ops
// ==========================================
#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_schema(lf_ptr: *mut LazyFrameContext) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &mut *lf_ptr };
        
        let schema = ctx.inner.collect_schema()?;
        
        let mut json_parts = Vec::new();
        for (name, dtype) in schema.iter() {
            let dtype_str = dtype.to_string();
            json_parts.push(format!("\"{}\": \"{}\"", name, dtype_str));
        }
        let json = format!("{{ {} }}", json_parts.join(", "));
        
        Ok(std::ffi::CString::new(json).unwrap().into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_explain(lf_ptr: *mut LazyFrameContext, optimized: bool) -> *mut c_char {
    ffi_try!({
        let ctx = unsafe { &*lf_ptr };
        
        let plan_str = ctx.inner.explain(optimized)?;
        
        Ok(std::ffi::CString::new(plan_str).unwrap().into_raw())
    })
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_free_string(ptr: *mut std::os::raw::c_char) {
    if !ptr.is_null() {
        unsafe { let _ = std::ffi::CString::from_raw(ptr); }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_clone(lf_ptr: *mut LazyFrameContext) -> *mut LazyFrameContext {
    let ctx = unsafe { &*lf_ptr };
    
    let new_lf = ctx.inner.clone();
    
    Box::into_raw(Box::new(LazyFrameContext { inner: new_lf }))
}

#[unsafe(no_mangle)]
pub extern "C" fn pl_lazy_frame_free(ptr: *mut LazyFrameContext) {
    ffi_try_void!({
        if !ptr.is_null() {
            unsafe { let _ = Box::from_raw(ptr); }
        }
        Ok(())
    })
}

// Define Callback：C# will return ArrowArrayStream Pointer
type StreamFactoryCallback = unsafe extern "C" fn(*mut core::ffi::c_void) -> *mut polars_arrow::ffi::ArrowArrayStream;
type DestroyUserDataCallback = unsafe extern "C" fn(*mut core::ffi::c_void); 
// Define scanner struct
struct CSharpStreamScanner {
    schema: SchemaRef,
    callback: StreamFactoryCallback,
    destroy_callback: Option<DestroyUserDataCallback>,
    user_data: *mut core::ffi::c_void, 
}

unsafe impl Send for CSharpStreamScanner {}
unsafe impl Sync for CSharpStreamScanner {}

impl Drop for CSharpStreamScanner {
    fn drop(&mut self) {
        if let Some(destroy) = self.destroy_callback {
            unsafe {
                destroy(self.user_data);
            }
        }
    }
}

impl AnonymousScan for CSharpStreamScanner {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn scan(&self, _scan_opts: AnonymousScanArgs) -> PolarsResult<DataFrame> {
        unsafe {
            // Call C# for new pointer for stream
            let stream_ptr = (self.callback)(self.user_data);
            
            if stream_ptr.is_null() {
                return Err(PolarsError::ComputeError("C# callback returned null stream".into()));
            }

            let ctx_ptr = super::eager::pl_dataframe_new_from_stream(stream_ptr);
            
            if ctx_ptr.is_null() {
                return Err(PolarsError::ComputeError("Failed to consume stream".into()));
            }

            let ctx = Box::from_raw(ctx_ptr);
            Ok(ctx.df) 
        }
    }

    // Tell Polars the schema of data
    fn schema(&self, _infer_schema_length: Option<usize>) -> PolarsResult<SchemaRef> {
        Ok(self.schema.clone())
    }

    fn allows_predicate_pushdown(&self) -> bool {
        false 
    }
    fn allows_projection_pushdown(&self) -> bool {
        true 
    }
    fn allows_slice_pushdown(&self) -> bool {
        true 
    }
}
use polars::prelude::{Field as PolarsField};
#[unsafe(no_mangle)]
pub unsafe extern "C" fn pl_lazy_frame_scan_stream(
    ptr_schema: *mut polars_arrow::ffi::ArrowSchema,
    callback: StreamFactoryCallback,
    destroy_callback: DestroyUserDataCallback,
    user_data: *mut core::ffi::c_void,
) -> *mut LazyFrameContext {
    ffi_try!({
        // Parse C Schema
        let field = unsafe { polars_arrow::ffi::import_field_from_c(&*ptr_schema)? };
        
        // Arrow Field -> Polars Schema
        let arrow_dtype = field.dtype; 
        
        let schema = match arrow_dtype {
            ArrowDataType::Struct(fields) => {
                let mut schema = Schema::with_capacity(fields.len());
                for f in fields {
                    let p_field = PolarsField::from(&f);
                    schema.insert(p_field.name, p_field.dtype);
                }
                Arc::new(schema)
            },
            _ => return Err(PolarsError::ComputeError("Schema must be a Struct".into())),
        };

        let scanner = CSharpStreamScanner {
            schema,
            callback,
            destroy_callback: Some(destroy_callback),
            user_data,
        };

        let lf = LazyFrame::anonymous_scan(
            std::sync::Arc::new(scanner),
            ScanArgsAnonymous::default()
        )?;

        Ok(Box::into_raw(Box::new(LazyFrameContext { inner: lf })))
    })
}

