use polars::prelude::*;
use polars_arrow::array::{BinaryArray, PrimitiveArray};
// use polars_arrow::bitmap::Bitmap;
// use polars_arrow::buffer::Buffer;
use polars_arrow::array::Array;
use polars_arrow::offset::OffsetsBuffer;
use polars_buffer::Buffer;

/// 核心：Z-Order 位交织器
/// 输入：一组 UInt32 类型的 Series (代表各列的 Rank 值)
/// 输出：一个 Binary 类型的 Series (代表 Z-Address)
pub fn interleave_columns(columns: &[Series]) -> PolarsResult<Series> {
    if columns.is_empty() {
        return Err(PolarsError::ComputeError("Z-Order requires at least one column".into()));
    }

    let row_count = columns[0].len();
    let num_cols = columns.len();
    
    // 1. 数据准备：将 Series 转为 Arrow Slice 以获得极致遍历性能
    // Step A: 确立所有权 (Owner)
    // 先把所有的 Series 转成 Arrow Array 并存起来，保证它们在内存中固定下来
    let mut _guards: Vec<Box<dyn Array>> = Vec::with_capacity(num_cols);
    
    for s in columns {
        if s.dtype() != &DataType::UInt32 {
            return Err(PolarsError::ComputeError(
                format!("Z-Order input must be UInt32, got {:?}", s.dtype()).into()
            ));
        }
        
        // Rechunk 确保内存连续
        let s_contiguous = s.rechunk();
        
        // 转为 Arrow Array (Box<dyn Array>)
        let arr = s_contiguous.to_arrow(0, CompatLevel::newest());
        
        // 存入 Guards，所有权转移到 Vec 中
        _guards.push(arr);
    }

    // Step B: 获取引用 (Borrower)
    // 现在 _guards 已经拥有了数据，我们可以安全地借用引用了
    let mut column_slices: Vec<&[u32]> = Vec::with_capacity(num_cols);

    for arr in &_guards {
        // 从 _guards 里借用 arr
        let primitive_arr = arr.as_any()
            .downcast_ref::<PrimitiveArray<u32>>()
            .ok_or_else(|| PolarsError::ComputeError("Failed to downcast to UInt32 array".into()))?;

        // 获取 slice 引用，生命周期绑定到 _guards
        column_slices.push(primitive_arr.values());
    }

    // 2. 计算输出大小
    // 每个输入 u32 有 32 位。总位数为 num_cols * 32。
    // 转为字节：(num_cols * 32) / 8 = num_cols * 4。
    let bytes_per_row = num_cols * 4;
    let total_bytes = row_count * bytes_per_row;
    
    // // 预分配大宽度的 Buffer
    // let mut value_buffer: Vec<u8> = Vec::with_capacity(total_bytes);
    // let mut offsets: Vec<i64> = Vec::with_capacity(row_count + 1);
    // let mut current_offset = 0;
    // offsets.push(0);

    // // 3. 硬核位操作循环 (The Hot Loop)
    // // 逻辑：对每一行，从 31 到 0 遍历每个 bit 位，
    // // 然后依次从 col 0 到 col N 提取该位，拼接到 output 中。
    
    // for i in 0..row_count {
    //     let mut byte_accumulator: u8 = 0;
    //     let mut bits_filled = 0;

    //     // 大端序：从高位 (31) 开始，保证生成的 Binary 字典序正确
    //     for bit_idx in (0..32).rev() {
    //         // 遍历每一列
    //         for col_idx in 0..num_cols {
    //             let val = column_slices[col_idx][i];
                
    //             // 提取第 bit_idx 位: (val >> bit_idx) & 1
    //             let bit = (val >> bit_idx) & 1;
                
    //             // 塞入累加器 (左移腾位置)
    //             byte_accumulator = (byte_accumulator << 1) | (bit as u8);
    //             bits_filled += 1;

    //             // 攒够 8 位，刷入 Buffer
    //             if bits_filled == 8 {
    //                 value_buffer.push(byte_accumulator);
    //                 byte_accumulator = 0;
    //                 bits_filled = 0;
    //             }
    //         }
    //     }
        
    //     current_offset += bytes_per_row as i64;
    //     offsets.push(current_offset);
    // }

    let mut value_buffer: Vec<u8> = vec![0u8; total_bytes]; 
    
    // Offsets 依然需要
    let mut offsets: Vec<i64> = Vec::with_capacity(row_count + 1);
    let mut current_offset = 0;
    for _ in 0..=row_count {
        offsets.push(current_offset);
        current_offset += bytes_per_row as i64;
    }

    // 3. 🚀 硬核批量位操作 (The Hot Loop - Batched)
    // 我们一次处理 LANES 行。这模拟了 SIMD 的行为，利用了 CPU 的超标量能力。
    const LANES: usize = 8;
    
    let mut i = 0;
    let dst_ptr_base = value_buffer.as_mut_ptr();

    // -- 主循环：处理能被 LANES 整除的部分 --
    while i + LANES <= row_count {
        // 定义 8 个累加器 (寄存器变量)
        let mut acc = [0u8; LANES];
        let mut bits_filled = 0;

        // 计算这 8 行的写入起始位置指针
        let mut dst_ptrs = [std::ptr::null_mut::<u8>(); LANES];
        for k in 0..LANES {
            // Safety: 我们已经预分配了 total_bytes，且 i+k < row_count
            unsafe {
                dst_ptrs[k] = dst_ptr_base.add((i + k) * bytes_per_row);
            }
        }

        // 遍历 Bit (31..0)
        for bit_idx in (0..32).rev() {
            // 遍历 Col
            for col_idx in 0..num_cols {
                let col_data = column_slices[col_idx];
                
                // Unroll: 获取 8 行的数据
                let vals = [
                    col_data[i], col_data[i+1], col_data[i+2], col_data[i+3],
                    col_data[i+4], col_data[i+5], col_data[i+6], col_data[i+7]
                ];

                // 提取位并累加 (这一步极易被 CPU 并行化)
                for k in 0..LANES {
                    let bit = (vals[k] >> bit_idx) & 1;
                    acc[k] = (acc[k] << 1) | (bit as u8);
                }
                
                bits_filled += 1;

                // 攒够 8 位，写入内存
                if bits_filled == 8 {
                    for k in 0..LANES {
                        unsafe {
                            *dst_ptrs[k] = acc[k]; // 写入 byte
                            dst_ptrs[k] = dst_ptrs[k].add(1); // 指针后移
                            acc[k] = 0; // 重置累加器
                        }
                    }
                    bits_filled = 0;
                }
            }
        }
        i += LANES;
    }

    // -- 剩余循环：处理剩下的不足 8 行的数据 (Fallback) --
    while i < row_count {
        let mut byte_accumulator: u8 = 0;
        let mut bits_filled = 0;
        
        // 计算当前行的写入指针
        let mut current_dst_ptr = unsafe { dst_ptr_base.add(i * bytes_per_row) };

        for bit_idx in (0..32).rev() {
            for col_idx in 0..num_cols {
                let val = column_slices[col_idx][i];
                let bit = (val >> bit_idx) & 1;
                byte_accumulator = (byte_accumulator << 1) | (bit as u8);
                bits_filled += 1;

                if bits_filled == 8 {
                    unsafe {
                        *current_dst_ptr = byte_accumulator;
                        current_dst_ptr = current_dst_ptr.add(1);
                    }
                    byte_accumulator = 0;
                    bits_filled = 0;
                }
            }
        }
        i += 1;
    }

    // 4. 构建 Polars Binary Series
    // Safety: 我们手动构建了合法的 Arrow BinaryArray
    let arrow_array = unsafe {
        // Step A: Vec<u8> -> Buffer<u8>
        let values_buf = Buffer::from(value_buffer);

        // Step B: Vec<i64> -> Buffer<i64> -> OffsetsBuffer<i64>
        // 以前可以直接 into()，现在必须显式包装。
        // 使用 new_unchecked 是因为我们在上面的循环里保证了 offsets 是单调递增且以 0 开头的，
        // 没必要让它再 O(N) 检查一遍。
        let offsets_buf = Buffer::from(offsets);
        let offsets_arrow = OffsetsBuffer::new_unchecked(offsets_buf);

        // Step C: 构建 BinaryArray
        BinaryArray::<i64>::new_unchecked(
            DataType::Binary.to_arrow(CompatLevel::newest()), 
            offsets_arrow, // 这里传入 OffsetsBuffer 类型
            values_buf,    // 这里传入 Buffer 类型
            None,          // No nulls
        )
    };

    // 这里 Series::from_arrow 可能会把 Box<dyn Array> 包装回去
    let series = Series::from_arrow("z_address".into(), Box::new(arrow_array))?;
    Ok(series)
}

// ------------------------------------------------------------------
// Helper: Apply Z-Order to LazyFrame
// ------------------------------------------------------------------

/// 对 LazyFrame 应用 Z-Order 排序
/// columns: 需要参与 Z-Order 的列名
pub fn apply_z_order(lf: LazyFrame, columns: &[String]) -> PolarsResult<LazyFrame> {
    if columns.is_empty() {
        return Ok(lf);
    }

    let cols_owned = columns.to_vec();
    
    // FIX: 闭包签名适配 Polars 0.53+ (Column -> PolarsResult<Column>)
    let z_order_map_func = move |c: Column| {
        // 1. Column -> Series
        // Column 实现了 Deref<Target=Series>，或者可以用 .as_materialized_series()
        // 但通常直接把它当 Series 用即可
        let s = c.as_materialized_series();

        // 2. 解包 Struct
        let ca = s.struct_()?;
        
        // 3. 获取字段 (Vec<Series>)
        // Polars 0.53+ 使用 fields_as_series() 获取数据
        let fields = ca.fields_as_series(); 
        
        // 4. 计算位交织 (返回 Series)
        let result_series = interleave_columns(&fields)?;
        
        // 5. Series -> Column
        Ok(Column::from(result_series))
    };

    // 2. Output Type Function (元数据推断): (&Schema, &Field) -> PolarsResult<Field>
    // 这就是源码里要求的 DT
    let z_order_output_type_func = move |_schema: &Schema, input_field: &Field| {
        // 我们不关心输入是什么结构，输出一定是一个 Binary 类型的字段
        // 名字通常沿用输入的，或者由后续的 alias 覆盖
        Ok(Field::new(input_field.name().clone(), DataType::Binary))
    };

    let z_col_name = "__z_order_addr";
    let z_col_pl_str = PlSmallStr::from_static(z_col_name);
    
    // 构建 Rank 表达式
    let rank_exprs: Vec<Expr> = cols_owned.iter()
        .map(|name| {
            col(name)
                .rank(RankOptions {
                    method: RankMethod::Dense,
                    descending: false,
                }, None)
                .cast(DataType::UInt32)
                .alias(name)
        })
        .collect();

    // 构建流程
    let lf_sorted = lf
        .with_column(
            as_struct(rank_exprs)
                .map(
                    z_order_map_func, 
                    z_order_output_type_func 
                )
                .alias(z_col_name)
        )
        .sort(
            [z_col_pl_str.clone()], 
            SortMultipleOptions::default()
        )
        .drop(Selector::ByName { 
            names: Arc::from([PlSmallStr::from_static(z_col_name)]), 
            strict: true 
        });

    Ok(lf_sorted)
}