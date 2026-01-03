namespace Polars.FSharp.Tests

module UdfLogic =
    open Apache.Arrow

    // 场景 A: 类型转换 (Int32 -> String)
    let intToString (arr: IArrowArray) : IArrowArray =
        match arr with
        | :? Int64Array as i64Arr ->
            let builder = new StringViewArray.Builder()
            for i in 0 .. i64Arr.Length - 1 do
                if i64Arr.IsNull(i) then builder.AppendNull() |> ignore
                else 
                    let v = i64Arr.GetValue(i).Value
                    builder.Append $"Value: {v}" |> ignore
            builder.Build() :> IArrowArray

    // 为了兼容性保留 Int32
        | :? Int32Array as i32Arr ->
            let builder = new StringViewArray.Builder()
            for i in 0 .. i32Arr.Length - 1 do
                if i32Arr.IsNull i then 
                    builder.AppendNull() |> ignore
                else 
                    let v = i32Arr.GetValue(i).Value
                    builder.Append $"Value: {v}" |> ignore
            builder.Build() :> IArrowArray
        |_ -> failwith $"Expected Int32Array or Int64Array, but got: {arr.GetType().Name}"

    // 场景 B: 必定报错
    let alwaysFail (arr: IArrowArray) : IArrowArray =
        failwith "Boom! C# UDF Exploded!"

open Xunit
open Polars.FSharp
open Apache.Arrow
open System

type ``UDF Tests`` () =

    [<Fact>]
    member _.``Map UDF can change data type (Int -> String)`` () =
        // 1. 准备数据
        use csv = new TempCsv "num\n100\n200"
        let lf = LazyFrame.ScanCsv csv.Path
        
        // 2. 构造 C# 委托
        let udf = Func<IArrowArray, IArrowArray> UdfLogic.intToString

        // 3. 执行 Polars 查询
        // 关键点：必须传入 PlDataType.String，否则 Polars 可能会把结果当成 Int 处理导致乱码
        let df = 
            lf 
            |> pl.withColumnLazy (
                pl.col "num"
                |> fun e -> e.Map(udf, DataType.String)
                |> pl.alias "desc"
            )
            |> pl.selectLazy [ pl.col "desc" ]
            |> pl.collect

        // 4. 验证结果
        let arrowBatch = df.ToArrow()
        let strCol = arrowBatch.Column "desc" :?> StringViewArray
        
        Assert.Equal("Value: 100", strCol.GetString 0)
        Assert.Equal("Value: 200", strCol.GetString 1)

    [<Fact>]
    member _.``Map UDF error is propagated to F#`` () =
        // 1. 准备数据
        use csv = new TempCsv("num\n1")
        let lf = LazyFrame.ScanCsv csv.Path
        
        let udf = Func<IArrowArray, IArrowArray> UdfLogic.alwaysFail

        // 2. 断言会抛出异常
        let ex = Assert.Throws<Exception>(fun () -> 
            lf 
            |> pl.withColumnLazy (
                pl.col "num" 
                |> fun e -> e.Map(udf, DataType.SameAsInput)
            )
            // UDF 是 Lazy 执行的，只有 Collect/ToArrow 时才会触发
            |> pl.collect 
            |> ignore
        )

        // 3. 验证异常信息
        // 我们期望看到 C# 的异常信息包含在 PolarsError 里
        Assert.Contains("Boom! C# UDF Exploded!", ex.Message)
        Assert.Contains("C# UDF Failed", ex.Message) // 这是 Rust 代码里加的前缀

    [<Fact>]
    member _.``Generic Map UDF with Lambda (Int -> String)`` () =
        use csv = new TempCsv "num\n100\n"
        let lf = LazyFrame.ScanCsv csv.Path
        
        // --- 用户的代码极度简化 ---
        // 1. 定义一个简单的匿名函数 (int -> string)
        let myLogic = fun (x: int) -> sprintf "Num: %d" (x + 1)

        let df = 
            lf 
            |> pl.withColumnLazy (
                pl.col "num"
                // 2. 直接调用 Udf.map
                // 泛型 'T 和 'U 会自动推断为 int 和 string
                |> fun e -> e.Map(Udf.map myLogic, DataType.String) 
                |> pl.alias "res"
            )
            |> pl.selectLazy [ pl.col "res" ]
            |> pl.collect

        // 验证
        let arrow = df.ToArrow()
        let col = arrow.Column "res" :?> StringViewArray // 自动用了 StringView
        
        Assert.Equal("Num: 101", col.GetString 0)
        Assert.Equal(1, col.Length)
    [<Fact>]
    member _.``UDF: Map with Option (Null Handling)`` () =
        // 数据: [10, 20, null]
        // 注意: CSV 最后一行写 null 还是空行取决于解析器，这里用空行配合 Polars 默认行为
        use csv = new TempCsv "val\n10\n20\n" 
        let lf = LazyFrame.ScanCsv csv.Path

        // 逻辑: 
        // 输入是 int option
        // 如果有值且 > 15 -> 返回 Some (x * 2)
        // 否则 (<= 15 或原本就是 null) -> 返回 None (即 null)
        let logic (opt: int option) =
            match opt with
            | Some x when x > 15 -> Some (x * 2)
            | _ -> None

        let df = 
            lf 
            |> pl.withColumnLazy (
                pl.col "val"
                // 使用 mapOption，显式处理 Option 类型
                |> fun e -> e.Map(Udf.mapOption logic, DataType.Int32)
                |> pl.alias "res"
            )
            |> pl.collect

        // 验证
        let arrow = df.ToArrow()
        let col = arrow.Column "res" :?> Int32Array // 注意根据 DataType.Int32 生成的是 Int32Array

        // Row 0: 10 -> (<=15) -> None
        Assert.True(col.IsNull 0)

        // Row 1: 20 -> (>15) -> 40
        Assert.Equal(40, col.GetValue(1).Value)

        // Row 2: null -> (None) -> None
        Assert.True(col.IsNull 2)
    [<Fact>]
    member _.``UDF: Decimal Map (Series Native)`` () =
        // 1. 准备数据: String ["10.50", "20.25", null]
        let data = ["10.50"; "20.25"; null]
        let s = Series.create("str_vals", data)

        // 2. 先 Cast 成 Decimal(10, 2)
        // Series.Cast 直接返回一个新的 Series
        let sDec = s.Cast(DataType.Decimal(Some 10, Some 2))

        // 3. 定义业务逻辑
        // 输入: decimal option
        // 输出: decimal option (乘以 2)
        let logic (opt: decimal option) =
            opt |> Option.map (fun d -> d * 2m)

        // 4. 直接在 Series 上执行 Map!
        // 彻底告别 ToFrame -> withColumn -> Expr.Map 的绕路写法
        // 这里利用了我们刚加的 Series.Map 泛型重载 (内部自动调用 Udf.mapOption)
        let res = sDec.MapOption(logic, DataType.Decimal(Some 10, Some 2))

        // 5. 验证结果
        // 咱们现在的 Series.GetValue 已经非常智能了，直接取值即可
        
        // 10.50 * 2 = 21.00
        Assert.Equal(21.00m, res.GetValue<decimal> 0)
        
        // 20.25 * 2 = 40.50
        Assert.Equal(40.50m, res.GetValue<decimal> 1)
        
        // null -> null
        // 验证 Option 返回 None
        Assert.True(res.GetValue<decimal option>(2).IsNone)
    [<Fact>]
    member _.``Series: Map (Basic UDF)`` () =
        // Data: [1, 2, 3]
        let s = Series.create("nums", [1; 2; 3])

        // Logic: x * 10
        let doubleFunc (x: int) = x * 10
        
        // 1. 编译 UDF (类型推断: int -> int)
        let udf = Udf.map doubleFunc

        // 2. Apply to Series
        // 我们明确知道返回是 Int32
        let sRes = s.Map(udf, DataType.Int32)

        // 3. Verify
        Assert.Equal(10, sRes.GetValue<int> 0)
        Assert.Equal(30, sRes.GetValue<int> 2)

    [<Fact>]
    member _.``Series: Map (Option Handling)`` () =
        // Data: [10, null, 30]
        let s = Series.create("vals", [Some 10; None; Some 30])

        // Logic: Some(x) -> Some(x + 1), None -> Some(-1)
        // 这是一个只有 mapOption 才能做到的 "FillNull with Logic"
        let logic (opt: int option) =
            match opt with
            | Some x -> Some (x + 1)
            | None -> Some -1

        // Apply
        let sRes = s.Map(Udf.mapOption logic, DataType.Int32)

        // Verify
        Assert.Equal(11, sRes.GetValue<int> 0)
        Assert.Equal(-1, sRes.GetValue<int> 1) // None became -1

    [<Fact>]
    member _.``Series: Map (F# Lambda Sugar)`` () =
        // Data: ["a", "b"]
        let s = Series.create("txt", ["a"; "b"])

        // 直接传 lambda，享受 F# 的丝滑
        // 逻辑: string -> string (加后缀)
        // 注意：这里需要显式指明泛型参数，或者通过类型注解让编译器推断 'T 和 'U
        // 因为 Series 本身不知道自己存的是 string，所以 'T 必须匹配物理类型
        let sRes = s.Map<string, string>((fun x -> x + "_suffix"), DataType.String)
        
        Assert.Equal("a_suffix", sRes.GetValue<string> 0)