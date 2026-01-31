namespace Polars.FSharp.Tests

open System
open System.IO
open Xunit
open Polars.FSharp

type ScanParquetTests() =
    
    // 准备一个临时文件路径
    let tempPath = Path.Combine(Path.GetTempPath(), $"scan_test_{Guid.NewGuid()}.parquet")

    // 在构造函数中生成一个标准的 Parquet 文件
    do
        let df = 
            DataFrame.create [
                Series.create("id", [1; 2; 3; 4; 5])
                Series.create("val", [10.5; 20.5; 30.5; 40.5; 50.5])
                Series.create("cat", ["A"; "B"; "A"; "B"; "C"])
            ]
        // 假设你有 WriteParquet，如果没有就用 CSV 替代，原理一样
        df.WriteParquet tempPath |> ignore

    // 析构函数清理垃圾
    interface IDisposable with
        member _.Dispose() =
            if File.Exists tempPath then File.Delete tempPath

    [<Fact>]
    member _.``ScanParquet (File): nRows and RowIndex``() =
        // 测试核心：
        // 1. nRows (传下去是指针) -> 生效应该只剩 3 行
        // 2. rowIndexName (字符串) -> 应该多出一列 "idx"
        // 3. rowIndexOffset (uint) -> "idx" 应该从 100 开始
        let lf = LazyFrame.ScanParquet(
            tempPath, 
            nRows = 3L, 
            rowIndexName = "idx", 
            rowIndexOffset = 100u
        )
        
        let df = lf.Collect()

        // Assert nRows
        Assert.Equal(3L, df.Height)
        
        // Assert Row Index exists and correct
        let idxCol = df.Column("idx")
        Assert.Equal(100u, idxCol.GetValue<uint32>(0))
        Assert.Equal(101u, idxCol.GetValue<uint32>(1))
        
        // Assert Data
        Assert.Equal(1, df.Column("id").GetValue<int>(0))

    [<Fact>]
    member _.``ScanParquet (Memory): Bytes and Schema Overwrite``() =
        // 1. 读取 bytes
        let bytes = File.ReadAllBytes(tempPath)

        // 2. 构造一个 PolarsSchema
        // 我们故意只定义两列，看看 Schema Projection 是否生效，或者验证 handle 传递是否成功
        // 注意：scan_parquet 的 schema 参数通常用于 overwrite 数据类型，
        // 这里我们主要验证 C# Wrapper 的 SafeHandleLock 逻辑没炸。
        use mySchema = new PolarsSchema([
            "id", DataType.Int32
            "val", DataType.Float64
            "cat", DataType.String
        ])

        // 3. 调用内存版 ScanParquet
        // 传入 schema，lowMemory=true 测试 bool flag
        let lf = LazyFrame.ScanParquet(
            bytes,
            schema = mySchema,
            lowMemory = true,
            useStatistics = false // 故意关掉 stats 测参数
        )

        let df = lf.Collect()

        // 验证读取完整性
        Assert.Equal(5L, df.Height)
        Assert.Equal("A", df.Column("cat").GetValue<string>(0))
        Assert.Equal(50.5, df.Column("val").GetValue<double>(4))

    [<Fact>]
    member _.``ScanParquet: Schema handle disposal safety``() =
        // 这是一个压力/安全性测试
        // 验证在 F# scope 结束销毁 Schema 后，ScanParquet 内部会不会因为持有悬空指针而崩溃
        // (你的 C# Wrapper 应该在调用 native 期间锁住 handle，这是关键)
        
        let bytes = File.ReadAllBytes tempPath
        let mutable lf = Unchecked.defaultof<LazyFrame>

        // Scope
        let runScope () =
            use schema = new PolarsSchema([
                "id", DataType.Int32
                "val", DataType.Float64
                "cat", DataType.String
                ])
            // 这里传入 schema，LazyFrame 建立 Logical Plan
            lf <- LazyFrame.ScanParquet(bytes, schema=schema)
            // schema 在这里 dispose
        
        runScope()

        // 此时 schema 已经 Dispose 了。
        // 如果 Polars (Rust) 在 scan 时只是拷贝了 schema 信息（通常是的），那么 collect 应该成功。
        // 如果 Rust 实际上持有 schema 的引用（不太可能，通常是指针拷贝），这里会炸。
        // 这个测试主要验证我们的 C# SafeHandleLock 是否正确延长了生命周期直到 Native 调用结束。
        let df = lf.Collect()
        Assert.True(df.Height > 0L)