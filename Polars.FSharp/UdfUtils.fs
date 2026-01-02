namespace Polars.FSharp

open System
open Apache.Arrow
open Polars.NET.Core.Arrow 
open Polars.NET.Core.Data 

module Udf =

    // ==========================================
    // 1. 普通 Map (T -> U)
    // ==========================================
    // 对应 C# UdfUtils.Wrap<TIn, TOut>
    // 自动处理 Null：如果输入是 Null 且 T 是值类型，则跳过计算直接输出 Null
    let map (f: 'T -> 'U) : Func<IArrowArray, IArrowArray> =
        Func<IArrowArray, IArrowArray>(fun inputArray ->
            let len = inputArray.Length

            // 1. Reader: 从 Core 获取高性能读取器 (int -> object)
            // 它负责处理 Arrow 复杂的内存布局和类型转换 (如 Int32Array -> int64)
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)

            // 2. Writer: 从 Core 获取智能缓冲池
            // 它会自动根据 'U 的类型选择正确的 Arrow Builder
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)

            // 3. 预检：T 是否为非空值类型 (如 int, double, 但不是 int? 或 string)
            // 如果是 int，遇到 null 时我们不能调用 f(null)，必须短路处理
            let tIn = typeof<'T>
            let isValueType = tIn.IsValueType && isNull (Nullable.GetUnderlyingType(tIn))

            for i in 0 .. len - 1 do
                // 检查 Null
                // 注意：ArrowReader.CreateAccessor 可能对某些 Null 返回 null，也可能不返回
                // 最稳妥的是直接问 IArrowArray
                if inputArray.IsNull(i) && isValueType then
                    // Case A: 输入是 Null，但函数要 int。直接输出 Null，不执行 f
                    buffer.Add(null)
                else
                    // Case B: 正常执行
                    // rawGetter(i) 已经把类型转好了，直接 unbox
                    let rawVal = rawGetter.Invoke(i)
                    let inputVal = unbox<'T> rawVal // 这里如果是 ref type (string) 且为 null，unbox 也是安全的
                    
                    // 执行用户逻辑
                    let outputVal = f inputVal
                    
                    // 写入 (ColumnBuffer.Add 支持泛型和 null)
                    buffer.Add(outputVal)

            // 4. 构建并返回 Arrow Array
            buffer.BuildArray()
        )

    // ==========================================
    // 2. Option Map (T option -> U option)
    // ==========================================
    // 允许用户自定义 Null 处理逻辑
    let mapOption (f: 'T option -> 'U option) : Func<IArrowArray, IArrowArray> =
        Func<IArrowArray, IArrowArray>(fun inputArray ->
            let len = inputArray.Length
            
            // 1. Reader
            let rawGetter = ArrowReader.CreateAccessor(inputArray, typeof<'T>)
            
            // 2. Writer (注意：目标类型是 'U，不是 'U option)
            // ColumnBufferFactory 会自动处理 'U 类型的 Null 写入
            let buffer = ColumnBufferFactory.Create(typeof<'U>, len)

            for i in 0 .. len - 1 do
                // 获取原始值 (object)
                let rawVal = rawGetter.Invoke(i)
                
                // 转换为 F# Option
                // 如果是 null -> None，否则 -> Some value
                let inputOpt = 
                    if isNull rawVal then None 
                    else Some (unbox<'T> rawVal)
                
                // 执行用户逻辑
                let outputOpt = f inputOpt
                
                // 写入 Buffer
                match outputOpt with
                | Some v -> buffer.Add(v)
                | None -> buffer.Add(null) // 显式写入 Null

            buffer.BuildArray()
        )