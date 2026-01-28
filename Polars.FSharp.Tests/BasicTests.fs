namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open System
open System.IO

type DisposableFile (extension: string, ?content: string) =
    let ext = if extension.StartsWith(".") then extension else "." + extension
    let path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString() + ext)
    
    do
        match content with
        | Some text -> File.WriteAllText(path, text) // 模式 A: 写入内容 (CSV/JSON)
        | None -> () // 模式 B: 仅生成路径 (Parquet/IPC Write)

    member _.Path = path

    interface IDisposable with
        member _.Dispose() =
            try 
                if File.Exists path then File.Delete path
            with _ -> ()
type UserRecord = {
        name: string
        age: int          // Int64 -> Int32
        score: float option // Nullable Float
        joined: System.DateTime option // Timestamp -> DateTime
    }

type TestUser = {
    Name: string
    Age: int
    Score: float
    IsActive: bool
    JoinDate: DateTime
}
[<CLIMutable>]
type SensorData = {
    Id: int
    Value: string
    Timestamp: DateTime
}

// 场景 2: 过滤测试
[<CLIMutable>]
type Student = {
    Id: int
    Group: string
    Score: double
}

// 场景 3: Join 测试 (Multi-pass)
[<CLIMutable>]
type JoinItem = {
    Key: int
    Val: int
}
type ``Basic Functionality Tests`` () =

    [<Fact>]
    member _.``Can read CSV and count rows/cols`` () =
        use csv = new TempCsv "name,age,birthday\nAlice,30,2022-11-01\nBob,25,2025-12-03"
        
        let df = DataFrame.ReadCsv (path=csv.Path)
        
        Assert.Equal(2L, df.Rows)    // 注意：现在 Rows 返回的是 long (int64)
        Assert.Equal(3L, df.Width) // 注意：现在 Width 返回的是 long
    [<Fact>]
    member _.``IO: Advanced CSV Reading (Schema, Skip, Dates)`` () =
        let path = "advanced_test.csv"
        try
            let content = 
                "IGNORE_THIS_LINE\n" +
                "id;date_col;val_col\n" +
                "007;2023-01-01;99.9\n" +
                "008;2023-12-31;10.5"
            System.IO.File.WriteAllText(path, content)

            // 调用 DataFrame.ReadCsv
            use df = DataFrame.ReadCsv(
                path,
                skipRows = 1,
                separator = ';',
                tryParseDates = true,
                schema = Map.ofList [
                    "id", DataType.String
                    // "date_col", DataType.Date   // 明确告诉它这是 Date
                    // "val_col", DataType.Float64 // 明确告诉它这是 Float
                ]
            )

            Assert.Equal(2L, df.Rows)
            Assert.Equal("str", df.Column("id").DtypeStr)
            Assert.Equal("007", df.String("id", 0).Value)
            Assert.Equal(99.9, df.Float("val_col", 0).Value)

        finally
            if File.Exists path then File.Delete path
    [<Fact>]
    member _.``Can read&write Parquet`` () =
        // 1. 准备 CSV 数据
        use csv = new DisposableFile(".csv", "a,b,c,d\n1,2,3,4")
        use df = DataFrame.ReadCsv(csv.Path, tryParseDates=false)
        
        // 2. 准备 Parquet 目标路径 (不预先创建文件)
        use parquet = new DisposableFile ".parquet"
        
        // 3. 写入
        // 注意：F# 方法返回 this，忽略它
        df.WriteParquet parquet.Path |> ignore
        
        // 4. 验证文件确实生成了
        Assert.True(File.Exists parquet.Path, $"Parquet file should exist at {parquet.Path}")

        // 5. 读回来验证内容
        use df2 = DataFrame.ReadParquet parquet.Path
        Assert.Equal(df.Rows, df2.Rows)
        Assert.Equal(4, df2.Schema.Count)

    [<Fact>]
    member _.``IO: Write & Read IPC/JSON`` () =
        // 准备路径托管
        use ipcFile = new DisposableFile ".ipc"
        use jsonFile = new DisposableFile ".json"

        // 1. 准备数据
        let s1 = Series.create("a", [1; 2; 3])
        let s2 = Series.create("b", ["x"; "y"; "z"])
        use df = DataFrame.create [s1; s2]

        // 2. 测试 IPC (Feather)
        df.WriteIpc ipcFile.Path |> ignore
        Assert.True(File.Exists ipcFile.Path, "IPC file not found")
        
        use dfIpc = DataFrame.ReadIpc ipcFile.Path
        Assert.Equal(3L, dfIpc.Rows)
        Assert.Equal("x", dfIpc.String("b", 0).Value)

        // 3. 测试 JSON
        df.WriteJson jsonFile.Path |> ignore
        Assert.True(File.Exists jsonFile.Path, "JSON file not found")
        
        use dfJson = DataFrame.ReadJson jsonFile.Path
        Assert.Equal(3L, dfJson.Rows)
        Assert.Equal(2L, dfJson.Int("a", 1).Value)
    [<Fact>]
    member _.``Streaming: Debug Sink`` () =
        // 1. 准备数据
        use csv = new DisposableFile(".csv", "a,b\n1,2\n3,4")
        use parquetEager = new DisposableFile ".parquet"
        use parquetSink = new DisposableFile ".parquet"

        printfn "CSV Path: %s" csv.Path
        printfn "Target Sink Path: %s" parquetSink.Path

        // Step A: 验证 LazyFrame 能否读取数据 (Collect)
        // 如果这里挂了，说明 scanCsv 没读到文件，或者 CSV 格式 Polars 不认
        let lf = LazyFrame.ScanCsv(csv.Path)
        use df = lf.Collect()
        
        Assert.Equal(2L, df.Rows)
        printfn "Step A: Collect Success. Rows: %d" df.Rows

        // Step B: 验证 Eager WriteParquet 是否正常
        // 如果这里挂了，说明是文件系统/权限问题，与 Lazy 无关
        df.WriteParquet(parquetEager.Path) |> ignore
        Assert.True(System.IO.File.Exists parquetEager.Path, "Step B: Eager Write Failed")
        printfn "Step B: Eager Write Success"

        // Step C: 验证 Lazy Sink (使用新的 LazyFrame)
        // 注意：之前的 lf 已经被 Collect 消耗了 (虽然我们 CloneHandle 了，但为了纯净环境重新 scan)
        LazyFrame.ScanCsv(csv.Path)
            .SinkParquet parquetSink.Path
    
        Assert.True(System.IO.File.Exists parquetSink.Path, "Step C: Lazy Sink Failed")
        printfn "Step C: Lazy Sink Success"
    [<Fact>]
    member _.``Metadata: Schema and Dtype`` () =
        // 1. 创建 DataFrame
        use s1 = Series.create("id", [1; 2; 3])
        use s2 = Series.create("score", [1.1; 2.2; 3.3])
        use s3 = Series.create("is_active", [true; false; true])
        
        use df = DataFrame.create [s1; s2; s3]

        // 2. 验证 Series Dtype
        Assert.Equal("i32", s1.DtypeStr)   // F# int 是 Int32
        Assert.Equal("f64", s2.DtypeStr) // F# float 是 double (Float64)
        Assert.Equal("bool", s3.DtypeStr)

        // 3. 验证 DataFrame Schema
        let schema = df.Schema
        Assert.Equal(3, schema.Count)
        Assert.Equal(DataType.Int32, schema.["id"])
        Assert.Equal(DataType.Float64, schema.["score"])
        Assert.Equal(DataType.Boolean, schema.["is_active"])
        
        // 4. 打印看看效果
        Console.WriteLine "------Test DataFrame PrintSchema START------"
        df.PrintSchema()
        Console.WriteLine "------Test DataFrame PrintSchema END------"
    [<Fact>]
    member _.``Lazy Introspection: Schema and Explain`` () =
        use csv = new TempCsv "a,b\n1,2"
        let lf = LazyFrame.ScanCsv (path=csv.Path, tryParseDates=false)
        
        let lf2 = 
            lf 
            |> pl.withColumnLazy (
                (pl.col "a" * pl.lit 2).Alias "a_double"
            )
            |> pl.filterLazy (pl.col "b" .> pl.lit 0)

        // 1. 验证 Schema (使用 Map API，更加精准)
        let schema = lf2.Schema 
        
        // 验证列名是否存在 (Key)
        Assert.True(schema.ContainsKey "a")
        Assert.True(schema.ContainsKey "b")
        Assert.True(schema.ContainsKey "a_double")
        
        // 验证列类型 (Value)
        // Polars 读取 CSV 整数默认是 Int64
        Assert.Equal(DataType.Int64, schema.["a"])
        Assert.Equal(DataType.Int64, schema.["b"])
        Assert.Equal(DataType.Int64, schema.["a_double"])

        // 2. 验证 Explain 和 Optimization
        let plan = lf2.Explain false
        printfn "\n=== Query Plan ===\n%s\n==================" plan
        Assert.Contains("FILTER", plan) 
        Assert.Contains("WITH_COLUMNS", plan)

        let planOptimized = lf2.Explain true
        printfn "\n=== Query Plan Optimized===\n%s\n==================" planOptimized
        Assert.Contains("SELECTION", planOptimized) 
    [<Fact>]
    member _.``Arrow Integration: Import C# Arrow Data to Polars`` () =
        // 1. 在 C# 端原生构建一个 RecordBatch
        // 模拟场景：数据来自 .NET 数据库或计算结果
        let builder = new Apache.Arrow.Int64Array.Builder()
        builder.Append 100L |> ignore
        builder.Append 200L |> ignore
        builder.AppendNull() |> ignore // 测试空值
        let colArray = builder.Build()

        let field = new Apache.Arrow.Field("num", new Apache.Arrow.Types.Int64Type(), true)
        let schema = new Apache.Arrow.Schema([| field |], null)
        
        use batch = new Apache.Arrow.RecordBatch(schema, [| colArray |], 3)

        // 2. 传给 Polars (C# -> Rust)
        // 这一步应该能成功，因为内存是 C# 分配的，Exporter 能够处理
        let df = DataFrame.FromArrow batch
        df |> pl.show |> ignore
        // 3. 验证
        Assert.Equal(3L, df.Rows)
        Assert.Equal(100L, df.Int("num", 0).Value)
        Assert.Equal(200L, df.Int("num", 1).Value)
        Assert.True(df.Int("num", 2).IsNone) // 验证空值传递
    [<Fact>]
    member _.``Series: AsSeq Lifecycle & Complex Types`` () =
    // 1. 创建包含复杂类型的 Series
        let data = [
            Some(DateTime(2023, 1, 1))
            None
            Some(DateTime(2024, 1, 1))
        ]
        use s = Series.ofSeq("dt", data)

        // 2. 惰性序列 (此时 Arrow Array 还没创建)
        let seqData = s.AsSeq<DateTime>()

        // 3. 触发迭代 (Arrow Array 创建 -> 读取 -> 释放)
        let listData = seqData |> Seq.toList

        Assert.Equal(3, listData.Length)
        Assert.Equal(Some(DateTime(2023, 1, 1)), listData.[0])
        Assert.True(listData.[1].IsNone)
    [<Fact>]
    member _.``Ingestion: Create DataFrame from F# Records`` () =
        // 1. 定义数据
        let data = [
            { name = "Alice"; age = 30; score = Some 99.5; joined = Some (DateTime(2023,1,1)) }
            { name = "Bob"; age = 25; score = None; joined = None }
        ]

        // 2. 转换 (ofRecords)
        let df = DataFrame.ofRecords data

        // 3. 验证结构
        Assert.Equal(2L, df.Rows)
        Assert.Equal(4L, df.Width)

        // 4. 验证数据
        
        // --- String (Alice) ---
        // [修复] df.String 返回 string option，必须用 .Value 取出里面的 string 才能和 "Alice" 比较
        Assert.Equal("Alice", df.String("name", 0).Value) 
        
        // --- Int (Alice) ---
        Assert.Equal(30L, df.Int("age", 0).Value) 
        
        // --- Float (Alice) ---
        Assert.Equal(99.5, df.Float("score", 0).Value)
        
        // --- DateTime (Alice) ---
        // [修复] 先取出 Option 里的值，再判断包含关系
        let joinedAlice = df.String("joined", 0).Value
        Assert.Contains("2023-01-01", joinedAlice)

        // --- Bob (验证 Null) ---
        Assert.Equal("Bob", df.String("name", 1).Value)
        Assert.Equal(25L, df.Int("age", 1).Value)
        
        // Score 是 None
        Assert.True(df.Float("score", 1).IsNone)
        
        // Joined 是 None
        // [修复] 不要用 = null 判断 Option，要用 .IsNone
        let joinedCol = df.Column("joined").AsSeq<DateTime option>()
        let joinedBob = Seq.item 1 joinedCol
        Assert.True joinedBob.IsNone
    [<Fact>]
    member _.``DataFrame: Create from Series`` () =
        // 1. 创建两个独立的 Series
        use s1 = Series.create("id", [1; 2; 3])
        use s2 = Series.create("name", ["a"; "b"; "c"])

        // 2. 组合成 DataFrame
        use df = DataFrame.create [s1; s2]

        // 3. 验证
        Assert.Equal(3L, df.Rows)
        Assert.Equal(2L, df.Width)
        Assert.Equal<string seq>(["id"; "name"], df.ColumnNames)
        
        // 4. [关键] 验证原来的 Series 依然可用 (未被 Move)
        // 如果 Rust 端不是 Clone 而是 Move，这里就会崩
        Assert.Equal(3L, s1.Length)
        
        // 5. 打印看看
        pl.show df |> ignore
    [<Fact>]
    member _.``Convenience: Drop, Rename, DropNulls, Sample`` () =
        // Test DataFrame
        let s1 = Series.create("a", [Some 1; Some 2; None])
        let s2 = Series.create("b", ["x"; "y"; "z"])
        use df = DataFrame.create [s1; s2]

        // 1. Drop
        let dfDrop = df.Drop "a"
        Assert.Equal(1L, dfDrop.Width)
        Assert.Equal<string seq>(["b"], dfDrop.ColumnNames)
        Assert.Equal(2L, df.Width) |> ignore
        Assert.Equal<string seq>(["a"; "b"], df.ColumnNames)

        // 2. Rename
        let dfRenamed = df.Rename("b", "b_new")
        Assert.Equal<string seq>(["a"; "b_new"], dfRenamed.ColumnNames)

        // 3. DropNulls
        let dfClean = df.DropNulls()
        Assert.Equal(2L, dfClean.Rows) // 第三行 a=null 被删了
        Assert.Equal(Some 1L, dfClean.Int("a", 0))
        Assert.Equal(Some 2L, dfClean.Int("a", 1))

        // 4. Sample (n=1)
        let dfSample = df.Sample(n=1, seed=12345UL)
        Assert.Equal(1L, dfSample.Rows)
        
        // 5. Sample (frac=0.5) -> 3 * 0.5 = 1.5 -> 1 or 2 rows depending on algo, usually round/floor
        // Polars sample_frac usually works well. 3 * 0.6 = 1.8. 
        // 让我们试个明确的
        let dfSampleFrac = df.Sample(frac=1.0) // 全量乱序
        Assert.Equal(3L, dfSampleFrac.Rows)
    [<Fact>]
    member _.``Full Temporal Types: Create & Retrieve`` () =
        let date = DateOnly(2023, 1, 1)
        let time = TimeOnly(12, 30, 0)
        let dur = TimeSpan.FromHours 1.5 // 90 mins

        // 1. Series Create
        use sDate = Series.create("d", [date])
        use sTime = Series.create("t", [time])
        use sDur = Series.create("dur", [dur])

        // 2. 验证类型 (使用强类型 DU，告别字符串魔法值)
        Assert.Equal(pl.date, sDate.DataType)
        Assert.Equal(pl.time, sTime.DataType)
        // Polars 默认 Duration 是 Microseconds
        Assert.Equal(DataType.Duration TimeUnit.Microseconds, sDur.DataType)

        // 3. 验证读取 (Scalar Access)
        Assert.Equal(date, sDate.GetValue<DateOnly> 0)
        Assert.Equal(time, sTime.GetValue<TimeOnly> 0)
        Assert.Equal(dur, sDur.GetValue<TimeSpan> 0)
        
        Assert.Equal(Some date, unbox<DateOnly option> sDate.[0])

        // 4. DataFrame.ofRecords 集成测试
        let records = [
            {| Id = 1; DoB = date; WakeUp = time; Shift = dur |}
        ]
        let df = DataFrame.ofRecords records
        
        // A. 列索引器: df.["ColName"] 返回 Series
        let sDoB = df.["DoB"]
        Assert.Equal(pl.date, sDoB.DataType)
        
        // B. 二维索引器: df.[row, col] 返回 obj
        Assert.Equal(Some date, df.Cell<DateOnly option>("DoB",0))
        Assert.Equal(Some time, df.Cell<TimeOnly option>("WakeUp",0))
        // 显式拆箱验证
        Assert.Equal(Some dur,  unbox<TimeSpan option> df.[0, "Shift"])
        
        // C. 验证 Option 支持 (GetValueOption)
        // 虽然这里数据不是 Option，但 API 应该能兼容返回 Some
        Assert.Equal(Some date, sDoB.GetValueOption<DateOnly> 0)
    [<Fact>]
    member _.``Expr: TimeZone Ops (Convert & Replace)`` () =
        // 1. 创建 Naive Datetime
        let s = Series.create("ts", [DateTime(2023, 1, 1, 12, 0, 0)])
        use df = DataFrame.create [s]

        let res = 
            df
            |> pl.select([
                pl.col "ts"
                
                // 1. Convert (Naive -> Error, so we must Replace first)
                // 先 Replace 设置为 UTC，再 Convert 到 Shanghai
                pl.col("ts")
                    .Dt.ReplaceTimeZone("UTC")
                    .Dt.ConvertTimeZone("Asia/Shanghai")
                    .Alias "shanghai"

                // 2. Replace with Strategy (Full signature check)
                // "raise" is default, explicitly passing it to verify API binding
                pl.col("ts")
                    .Dt.ReplaceTimeZone("Europe/London", ambiguous="earliest", nonExistent="null")
                    .Alias "london_explicit"
                
                // 3. Unset TimeZone (Make Naive)
                pl.col("ts")
                    .Dt.ReplaceTimeZone("UTC")
                    .Dt.ReplaceTimeZone(None) // Set back to None
                    .Alias "naive"
            ])

        // 验证 Shanghai (+08:00)
        let shRow = res.Column("shanghai").AsSeq<DateTimeOffset>() |> Seq.head |> Option.get
        Assert.Equal(TimeSpan.FromHours 8, shRow.Offset)
        Assert.Equal(20, shRow.Hour) // 12:00 UTC -> 20:00 Shanghai

        // 验证 London (Naive 12:00 -> London 12:00 +00:00 in Jan)
        let ldRow = res.Column("london_explicit").AsSeq<DateTimeOffset>() |> Seq.head |> Option.get
        Assert.Equal(0, ldRow.Offset.Hours) 
        
        // 验证 Naive (Unset)
        // 读取出来的应该是 UTC 的 DateTime,我们约定进入polars的一定是UTC
        let naiveRow = res.Column("naive").AsSeq<DateTime>() |> Seq.head |> Option.get
        Assert.Equal(DateTimeKind.Unspecified, naiveRow.Kind)
    [<Fact>]
    member _.``Conversion: DataFrame -> Lazy -> DataFrame`` () =
        // 1. 创建 Eager DF
        use df = DataFrame.ofRecords [ { name = "Qinglei"; age = 18 ; score = Some 99.5; joined = Some (System.DateTime(2023,1,1)) }; { name = "Someone"; age = 20; score = None; joined = None } ]
        
        // 2. 转 Lazy 并执行操作
        // 注意：df 在这里应该依然有效，因为 .Lazy() 是 Clone
        let lf = df.Lazy()
        
        let res = 
            lf
            |> pl.filterLazy(pl.col "age" .> pl.lit 18)
            |> pl.collect

        // 3. 验证结果
        Assert.Equal(1L, res.Rows)
        Assert.Equal(20L, res.Int("age", 0).Value)
        
        // 4. 验证原 DF 依然存活
        Assert.Equal(2L, df.Rows)
    [<Fact>]
    member _.``EDA: Describe (Manual Implementation)`` () =
        let s = Series.create("nums", [1.0; 2.0; 3.0; 4.0; 5.0])
        use df = DataFrame.create [s]

        let desc = df.Describe()
        
        pl.show desc |> ignore
        
        // 验证行数 (9个指标)
        Assert.Equal(9L, desc.Rows)
        
        // 验证 mean (第3行，第2列)
        // 注意：我们每一行是一个单独的 Select，顺序由 metrics 列表决定
        // 0: count, 1: null_count, 2: mean
        let meanVal = desc.Float("nums", 2).Value
        Assert.Equal(3.0, meanVal)
        
        // 验证 std
        let stdVal = desc.Float("nums", 3).Value
        // 1..5 的 std 是 1.5811...
        Assert.True(abs(stdVal - 1.58113883) < 0.0001)
    [<Fact>]
    member _.``Reshaping: Concat Diagonal`` () =
        // df1: [a, b]
        use csv1 = new TempCsv "a,b\n1,2"
        // df2: [a, c] (注意：没有 b，多了 c)
        use csv2 = new TempCsv "a,c\n3,4"

        let df1 = DataFrame.ReadCsv (path=csv1.Path, tryParseDates=false)
        let df2 = DataFrame.ReadCsv (path=csv2.Path, tryParseDates=false)

        // 对角拼接
        // 结果应该包含 3 列: [a, b, c]
        // Row 1 (来自 df1): a=1, b=2, c=null
        // Row 2 (来自 df2): a=3, b=null, c=4
        let res = pl.concat [df1; df2] Diagonal

        Assert.Equal(2L, res.Rows)
        Assert.Equal(3L, res.Width)
        
        // 验证列名
        let cols = res.ColumnNames
        Assert.Contains("a", cols)
        Assert.Contains("b", cols)
        Assert.Contains("c", cols)

        // 验证数据
        // 第一行 (df1)
        Assert.Equal(1L, res.Int("a", 0).Value)
        Assert.Equal(2L, res.Int("b", 0).Value)
        Assert.True(res.Int("c", 0).IsNone) // c 应该是 null

        // 第二行 (df2)
        Assert.Equal(3L, res.Int("a", 1).Value)
        Assert.True(res.Int("b", 1).IsNone) // b 应该是 null
        Assert.Equal(4L, res.Int("c", 1).Value)
    [<Fact>]
    member _.``Scalar Access: IsNullAt`` () =
        // 准备数据: [1, null, 3]
        use s = Series.create("a", [Some 1; None; Some 3])
        use df = DataFrame.create [s]

        // Series 验证
        Assert.False(s.IsNullAt 0)
        Assert.True(s.IsNullAt 1)
        Assert.False(s.IsNullAt 2)
        Assert.False(s.IsNullAt 999) // 越界返回 false

        // DataFrame 验证
        Assert.False(df.IsNullAt("a", 0))
        Assert.True(df.IsNullAt("a", 1))
    [<Fact>]
    member _.``Metadata: NullCount`` () =
        // 1. 创建包含 Null 的 Series
        // 数据: 1, null, 3, null
        let s = Series.create("a", [Some 1; None; Some 3; None])
        
        // 2. 验证 Series.NullCount
        Assert.Equal(2L, s.NullCount)
        Assert.Equal(4L, s.Length)

        // 3. 验证 DataFrame Helper
        use df = DataFrame.create [s]
        Assert.Equal(2L, df.NullCount "a")
 
    [<Fact>]
    member _.``Async: Collect LazyFrame`` () =
        // 构造一个稍微大一点的计算任务
        use csv1 = new TempCsv "a,b\n1,2\n3,4"
        let df = 
            LazyFrame.ScanCsv (path=csv1.Path, tryParseDates=false)
            |> pl.filterLazy (pl.col "a" .> pl.lit 0)
            |> pl.collectAsync // 返回 Async<DataFrame>
            |> Async.RunSynchronously // 在测试里阻塞等待结果

        Assert.Equal(2L, df.Rows)
        Assert.Equal(1L, df.Int("a", 0).Value)
    [<Fact>]
    member _.``Series: Arithmetic & Aggregation (Pandas Style)`` () =
        // 1. 准备数据
        use demand = Series.create("demand", [100.0; 200.0; 300.0])
        use weight = Series.create("weight", [0.5; 1.5; 1.0])

        // 2. Pandas 风格计算：加权平均
        // weighted_mean = (demand * weight).Sum() / weight.Sum()
        
        let sProd = demand * weight    // [50.0, 300.0, 300.0]
        let sSumProd = sProd.Sum()     // [650.0]
        let sSumW = weight.Sum()       // [3.0]
        
        // Series 之间的除法 (Broadcasting: Scalar / Scalar)
        let sWeightedMean = sSumProd / sSumW 
        
        // 结果应该是一个长度为1的 Series
        Assert.Equal(1L, sWeightedMean.Length)
        
        // 验证数值: 650 / 3 = 216.666...
        let valMean = sWeightedMean.Float(0).Value
        Assert.True(abs(valMean - 216.6666) < 0.001)

        // 3. 逻辑运算与过滤
        // weeks_with_demand = (demand > 0).sum()
        
        // (demand > 0) 返回 Boolean Series
        // .Sum() 在 Boolean Series 上通常等价于 count true，但 Polars Series Sum 可能返回 Int/Float
        // 让我们看看 boolean sum 的行为
        // Polars Rust boolean.sum() returns u32/u64 usually.
        
        let mask = demand .> 0.0 // 广播比较
        // pl.NET Sum() 返回的是 Series。对于 Bool，Rust sum 返回的是 number。
        // 我们验证一下类型
        let countPos = mask.Sum()
        // demand全是 > 0，所以应该是 3
        
        // 注意：Sum 返回的可能是 Int 或 Float，视底层实现而定
        // Polars boolean sum returns UInt32 usually.
        // 我们通过 .Float 或 .Int 尝试获取
        // 简单起见，先转 f64 再拿
        let countVal = countPos.Cast(DataType.Float64).Float(0).Value
        Assert.Equal(3.0, countVal)
        
        // zero_ratio = (demand == 0).mean()
        let zeroMask = demand .= 0.0
        let zeroRatio = zeroMask.Mean() // Mean on boolean = ratio of true
        
        // 0 / 3 = 0.0
        Assert.Equal(0.0, zeroRatio.Float(0).Value)
    [<Fact>]
    member _.``Series: Arithmetic & Aggregation (F# Pipeline Style)`` () =
        // 1. 准备数据
        use demand = Series.create("demand", [100.0; 200.0; 300.0])
        use weight = Series.create("weight", [0.5; 1.5; 1.0])

        // 2. F# 管道风格计算：加权平均
        // 逻辑流：demand 乘以 weight -> 求和 -> 除以 (weight 求和)
        
        let sWeightedMean = 
            demand
            |> Series.mul weight          // Element-wise multiplication
            |> Series.sum                 // Sum result
            |> Series.div (weight |> Series.sum) // Divide by scalar (series of len 1)

        // 验证
        Assert.Equal(1L, sWeightedMean.Length)
        let valMean = sWeightedMean.Float(0).Value
        Assert.True(abs(valMean - 216.6666) < 0.001)

        // 3. 逻辑运算与过滤
        
        // A. 统计需求大于 0 的周数
        // 逻辑流：demand -> 大于 0.0 -> 求和 -> 转 Float -> 取值
        let countVal = 
            demand
            |> Series.gtLit 0.0           // Broadcasting comparison (> 0.0)
            |> Series.sum                 // Count true values
            |> Series.cast DataType.Float64 
            |> fun s -> s.Float(0).Value  // 最后的取值也可以写个 helper

        Assert.Equal(3.0, countVal)

        // B. 统计零需求占比
        // 逻辑流：demand -> 等于 0.0 -> 求均值
        let zeroRatio = 
            demand
            |> Series.eqLit 0.0           // Broadcasting comparison (= 0.0)
            |> Series.mean                // Mean of boolean
            |> fun s -> s.Float(0).Value

        Assert.Equal(0.0, zeroRatio)
    [<Fact>]
    member _.``Series: NaN and Infinity Checks`` () =
        // 1. 准备数据: [1.0, NaN, Inf, -Inf, 5.0]
        let s = Series.create("f", [1.0; Double.NaN; Double.PositiveInfinity; Double.NegativeInfinity; 5.0])

        // 2. IsNan -> [F, T, F, F, F]
        let maskNan = s.IsNan()
        Assert.Equal(Some true, maskNan.Bool 1) // NaN
        Assert.Equal(Some false, maskNan.Bool 0)

        // 3. IsInfinite -> [F, F, T, T, F]
        let maskInf = s.IsInfinite()
        Assert.Equal(Some true, maskInf.Bool 2) // +Inf
        Assert.Equal(Some true, maskInf.Bool 3) // -Inf
        Assert.Equal(Some false, maskInf.Bool 1) // NaN is NOT Infinite

        // 4. IsFinite -> [T, F, F, F, T]
        let maskFin = s.IsFinite()
        Assert.Equal(Some true, maskFin.Bool 0)
        Assert.Equal(Some false, maskFin.Bool 1) // NaN not finite
        Assert.Equal(Some false, maskFin.Bool 2) // Inf not finite
    // ---------------------------------------------------
    // Streaming Tests
    // ---------------------------------------------------

    [<Fact>]
    member _.``Stream: Eager Ingestion (ofSeqStream)`` () =
        // 1. 模拟一个较大的数据源 (10万行)
        // 使用 Seq.init 惰性生成，不占内存
        let count = 100_000
        let data = Seq.init count (fun i -> 
            { Id = i; Value = $"Val_{i}"; Timestamp = DateTime(2023, 1, 1).AddSeconds(float i) }
        )

        // 2. 流式导入 (Batch Size = 10,000)
        // 这一步应该非常快，且内存占用极低
        use df = DataFrame.ofSeqStream(data, batchSize = 10_000)

        // 3. 验证
        Assert.Equal(int64 count, df.Rows)
        Assert.Equal("Val_99999", df.Column("Value").AsSeq<string>() |> Seq.last |> Option.get)
        let expectedType = Datetime(Microseconds, Some "")
        
        // 验证 Schema 是否正确推断 (Timestamp)
        Assert.Equal(expectedType, df.Schema.["Timestamp"])

    [<Fact>]
    member _.``Stream: Lazy Scan (scanSeq) with Filter`` () =
        // 1. 数据源
        let data = [
            { Id = 1; Group = "A"; Score = 10.0 }
            { Id = 2; Group = "B"; Score = 20.0 }
            { Id = 3; Group = "A"; Score = 30.0 }
        ]

        // 2. Lazy Scan -> Filter -> Collect
        // Polars 应该会将 Filter 下推，并在流式读取时就应用过滤
        let res = 
            LazyFrame.scanSeq data
                |> pl.filterLazy(pl.col "Group" .== pl.lit "A")
                |> pl.collectStreaming

        // 3. 验证
        Assert.Equal(2L, res.Rows) // 只剩下 Id 1 和 3
        Assert.Equal(1L, res.Int("Id", 0).Value)
        Assert.Equal(3L, res.Int("Id", 1).Value)

    [<Fact>]
    member _.``Stream: Lazy Multi-pass Scan (Self Join)`` () =
        // 这是一个的高级测试
        // Self Join 需要扫描同一份数据源两次
        // 这将验证我们的 ArrowStreamInterop.Factory 机制是否能正确重置枚举器
        
        let data = Seq.init 10 (fun i -> { Key = i % 3; Val = i }) // Key: 0, 1, 2 重复
        
        let lf = LazyFrame.scanSeq data
        
        // Self Join: lf.Join(lf, on="Key")
        // Rust 引擎会调用两次 StreamFactoryCallback
        let res = 
            lf
            |> pl.joinLazy lf [pl.col "Key"] [pl.col "Key"] JoinType.Left
            |> pl.collect

        // 简单验证行数 (笛卡尔积会膨胀)
        // 0: 4 items -> 4*4 = 16
        // 1: 3 items -> 3*3 = 9
        // 2: 3 items -> 3*3 = 9
        // Total = 34
        Assert.Equal(34L, res.Rows)
    [<Fact>]
    member _.``Series: Uniqueness and ApplyExpr`` () =
        // 1. 准备数据: [1, 2, 2, 3]
        let s = Series.create("nums", [1; 2; 2; 3])

        // 2. 验证 NUnique (Native)
        Assert.Equal(3UL, s.NUnique) // 1, 2, 3

        // 3. 验证 Unique (Native)
        let sUniq = s.Unique().Sort false // Sort to compare deterministically
        Assert.Equal(3L, sUniq.Length)
        Assert.Equal(1, sUniq.GetValue<int> 0)
        Assert.Equal(2, sUniq.GetValue<int> 1)
        Assert.Equal(3, sUniq.GetValue<int> 2)

        // 4. 验证 IsUnique (ApplyExpr)
        // [1, 2, 2, 3] -> IsUnique -> [T, F, F, T]
        let sIsUniq = s.IsUnique()
        Assert.Equal(4L, sIsUniq.Length)
        Assert.True(sIsUniq.GetValue<bool> 0)  // 1 is unique
        Assert.False(sIsUniq.GetValue<bool> 1) // 2 is not
        Assert.False(sIsUniq.GetValue<bool> 2) // 2 is not
        Assert.True(sIsUniq.GetValue<bool> 3)  // 3 is unique

        // 5. 验证 IsDuplicated (ApplyExpr)
        // [1, 2, 2, 3] -> IsDup -> [F, T, T, F]
        let sIsDup = s.IsDuplicated()
        Assert.False(sIsDup.GetValue<bool> 0)
        Assert.True(sIsDup.GetValue<bool> 1)
    [<Fact>]
    member _.``Series: Sort with Options (Nulls & Stable)`` () =
        // 1. 准备数据: [2, null, 1, 2]
        // 这里的 int option list 会被 Series.create 识别为 nullable int
        let s = Series.create("nums", [Some 2; None; Some 1; Some 2])

        // 2. 默认排序 (升序, Null 在前) -> [null, 1, 2, 2]
        // Polars 默认 nulls_last=false (Nulls are smallest)
        let sDef = s.Sort()
        Assert.True(sDef.IsNullAt 0)
        Assert.Equal(1,sDef .% 1)

        // 3. Nulls Last (升序, Null 在后) -> [1, 2, 2, null]
        let sNullLast = s.Sort(nullsLast = true)
        Assert.Equal(1, sNullLast .% 0)
        Assert.True(sNullLast.IsNullAt 3)

        // 4. 降序 + Nulls Last -> [2, 2, 1, null]
        let sDescNullLast = s.Sort(descending = true, nullsLast = true)
        Assert.Equal(2, sDescNullLast .% 0)
        Assert.True(sDescNullLast.IsNullAt 3)
        
        // 5. 稳定排序 (Maintain Order)
        // 虽然这里只有 4 个元素很难测出不稳定的情况，但 API 没崩就是胜利
        let sStable = s.Sort(maintainOrder = true)
        Assert.Equal(4L, sStable.Length)
    [<Fact>]
    member _.``DataFrame: Sort Advanced (Multi-Column & Nulls)`` () =
        // 1. 准备数据
        // Group: A, B
        // Val: 1, 2, null
        let data = [
            {| Group = "A"; Val = Some 1 |}
            {| Group = "A"; Val = None |}   // Null
            {| Group = "A"; Val = Some 2 |}
            {| Group = "B"; Val = Some 1 |}
        ]
        let df = DataFrame.ofRecords data

        // ==========================================
        // 场景 1: 简单全局排序 + Nulls Last
        // 需求: 按 Val 升序，Null 放最后
        // ==========================================
        let sorted1 = df.Sort( "Val", descending=false, nullsLast=true)
        
        // 预期: 1, 1, 2, null
        Assert.Equal(1, sorted1.Cell<int>("Val",0))
        Assert.True(sorted1.IsNullAt("Val",0) |> not)
        Assert.True(sorted1.IsNullAt("Val",3)) // 最后一个必须是 Null

        // ==========================================
        // 场景 2: 多列混合排序
        // 需求: Group 降序 (B->A), Val 升序 (1->2->null), Null Last
        // ==========================================
        let sorted2 = df.Sort(
            columns = [ pl.col "Group"; pl.col "Val" ],
            descending = [ true; false ],  // Group=Desc, Val=Asc
            nullsLast = [ false; true ]    // Group=Default, Val=Last
        )

        // 预期顺序:
        // 1. B, 1
        // 2. A, 1
        // 3. A, 2
        // 4. A, null

        // Row 0: B, 1
        Assert.Equal("B", sorted2.Cell<string>("Group",0))
        Assert.Equal(1, sorted2.Cell<int>("Val",0))

        // Row 3: A, null
        Assert.Equal("A", sorted2.Cell<string>("Group",3))
        Assert.True(sorted2.IsNullAt("Val",3))

    [<Fact>]
    member _.``LazyFrame: Sort Advanced`` () =
        let data = [
            {| A = 1; B = 3 |}
            {| A = 1; B = 1 |}
            {| A = 2; B = 2 |}
        ]
        let lf = DataFrame.ofRecords(data).Lazy()

        // 需求: A 升序, B 降序
        let res = 
            lf.Sort(
                [ pl.col "A"; pl.col "B" ], 
                descending = [false; true], 
                nullsLast = [false; false]
            ).Collect()

        // 预期:
        // 1. A=1, B=3
        // 2. A=1, B=1
        // 3. A=2, B=2
        
        Assert.Equal(1, res.Cell<int>("A",0))
        Assert.Equal(3, res.Cell<int>("B",0)) // B=3 排在 B=1 前面
        
        Assert.Equal(1, res.Cell<int>("A",1))
        Assert.Equal(1, res.Cell<int>("B",1))
    [<Fact>]
    member _. ``ScanSeq Streaming Mode - Should convert data correctly`` () =
        // Arrange
        let now = DateTime.Now
        let data = [
            { Name = "Alice"; Age = 25; Score = 99.5; IsActive = true; JoinDate = now }
            { Name = "Bob";   Age = 30; Score = 85.0; IsActive = false; JoinDate = now.AddDays -1.0 }
            { Name = "Charlie"; Age = 35; Score = 70.0; IsActive = true; JoinDate = now.AddDays -10.0 }
        ]

        // Act
        // 使用 Streaming 模式 (默认)
        use lf = LazyFrame.scanSeq data
        
        // 触发计算
        use df = lf.Collect()

        // Assert
        Assert.Equal(3L, df.Height)
        
        // 验证第一行的值
        let row0_Name = df.["Name"].[0] :?> string option
        let row0_Score = df.["Score"].[0] :?> double option
        
        // 验证 Some 值
        Assert.Equal(Some "Alice", row0_Name)
        Assert.Equal(Some 99.5, row0_Score)

    [<Fact>]
    member _. ``ScanSeq Buffered Mode - Should handle IO correctly`` () =
        // Arrange
        let data = seq {
            for i in 1 .. 1000 do
                yield { 
                    Name = sprintf "User_%d" i
                    Age = i
                    Score = float i * 1.5
                    IsActive = i % 2 = 0
                    JoinDate = DateTime.Now 
                }
        }

        // Act
        // 显式开启 Buffered 模式 (useBuffered = true)
        // 'use' 关键字会自动调用我们通过对象表达式内联的 Dispose 方法，清理临时文件
        use lf = LazyFrame.scanSeq(data, useBuffered = true, batchSize = 100)
        
        use df = lf.Collect()

        // Assert
        Assert.Equal(1000L, df.Height)
        
        let lastRowScore = df.["Score"].[999] :?> double option
        
        Assert.Equal(Some (1000.0 * 1.5), lastRowScore)

    [<Fact>]
    member _.``ScanSeq Empty Stream - Should preserve Schema without crashing`` () =
        // Arrange
        let emptyData = Seq.empty<TestUser>

        // Act
        // 这里测试的是我们在 C# 端做的 "Lazy Fallback" 逻辑
        // 即便数据源为空，也不应该抛出 AccessViolation，且必须返回正确的 Schema
        use lf = LazyFrame.scanSeq emptyData
        
        use df = lf.Collect()

        // Assert
        Assert.Equal(0L, df.Height)
        
        // 验证 Schema 是否存在 (如果 Schema 丢了，列名就不存在了，或者这里会抛错)
        let columns = df.ColumnNames
        Assert.Contains("Name", columns)
        Assert.Contains("Age", columns)
        Assert.Contains("Score", columns)
        Assert.Contains("IsActive", columns)
        Assert.Contains("JoinDate", columns)

type LitTests() =

    // 创建一个只有一行的 Dummy DataFrame 用于上下文执行 Expr
    let df = DataFrame.create [ Series.create("dummy", [1]) ]

    [<Fact>]
    member _.``Lit: All Primitive Types (Explicit Overload Check)``() =
        
        // ==========================================
        // 1. Signed Integers
        // ==========================================
        
        // int8 (sbyte) -> suffix 'y'
        let v_i8 = 123y
        let res_i8 = df.Select(pl.lit v_i8).Column(0).GetValue<sbyte>(0)
        Assert.Equal(v_i8, res_i8)

        // int16 (short) -> suffix 's'
        let v_i16 = 12345s
        let res_i16 = df.Select(pl.lit v_i16).Column(0).GetValue<int16>(0)
        Assert.Equal(v_i16, res_i16)

        // int32 (int)
        let v_i32 = 123456789
        let res_i32 = df.Select(pl.lit v_i32).Column(0).GetValue<int>(0)
        Assert.Equal(v_i32, res_i32)

        // int64 (long) -> suffix 'L'
        let v_i64 = 1234567890123456789L
        let res_i64 = df.Select(pl.lit v_i64).Column(0).GetValue<int64>(0)
        Assert.Equal(v_i64, res_i64)

        // Int128 (.NET 7+)
        let v_i128 = Int128.Parse "1000000000000000000000000000000" // 足够大以超过 int64
        let res_i128 = df.Select(pl.lit v_i128).Column(0).GetValue<Int128>(0)
        Assert.Equal(v_i128, res_i128)

        // ==========================================
        // 2. Unsigned Integers
        // ==========================================

        // uint8 (byte) -> suffix 'uy'
        let v_u8 = 200uy
        let res_u8 = df.Select(pl.lit v_u8).Column(0).GetValue<byte>(0)
        Assert.Equal(v_u8, res_u8)

        // uint16 (ushort) -> suffix 'us'
        let v_u16 = 40000us
        let res_u16 = df.Select(pl.lit v_u16).Column(0).GetValue<uint16>(0)
        Assert.Equal(v_u16, res_u16)

        // uint32 (uint) -> suffix 'u'
        let v_u32 = 3000000000u
        let res_u32 = df.Select(pl.lit v_u32).Column(0).GetValue<uint>(0)
        Assert.Equal(v_u32, res_u32)

        // uint64 (ulong) -> suffix 'UL'
        let v_u64 = 10000000000000000000UL
        let res_u64 = df.Select(pl.lit v_u64).Column(0).GetValue<uint64>(0)
        Assert.Equal(v_u64, res_u64)

        // ==========================================
        // 3. Floating Point & Decimal
        // ==========================================

        // float32 (single) -> suffix 'f'
        let v_f32 = 123.456f
        let res_f32 = df.Select(pl.lit v_f32).Column(0).GetValue<float32>(0)
        Assert.Equal(v_f32, res_f32)

        // float64 (double)
        let v_f64 = 123.456789123
        let res_f64 = df.Select(pl.lit v_f64).Column(0).GetValue<double>(0)
        Assert.Equal(v_f64, res_f64)

        // decimal -> suffix 'm'
        let v_dec = 123.456789m
        let res_dec = df.Select(pl.lit v_dec).Column(0).GetValue<decimal>(0)
        Assert.Equal(v_dec, res_dec)

        // ==========================================
        // 4. String & Boolean
        // ==========================================

        // string
        let v_str = "Polars.NET 🚀"
        let res_str = df.Select(pl.lit v_str).Column(0).GetValue<string>(0)
        Assert.Equal(v_str, res_str)

        // bool
        let v_bool = true
        let res_bool = df.Select(pl.lit v_bool).Column(0).GetValue<bool>(0)
        Assert.True res_bool

        // ==========================================
        // 5. Temporal Types
        // ==========================================

        // DateTime
        let v_dt = DateTime(2023, 10, 1, 12, 30, 45)
        let res_dt = df.Select(pl.lit v_dt).Column(0).GetValue<DateTime>(0)
        Assert.Equal(v_dt, res_dt)

        // DateOnly
        let v_date = DateOnly(2023, 10, 1)
        let res_date = df.Select(pl.lit v_date).Column(0).GetValue<DateOnly>(0)
        Assert.Equal(v_date, res_date)

        // TimeOnly
        let v_time = TimeOnly(12, 30, 45)
        // 注意：TimeOnly 在 Polars 中通常对应纳秒精度，可能存在微小浮点误差，
        // 但 Lit 生成是精确的，GetValue 只要也是精确的就行。
        let res_time = df.Select(pl.lit v_time).Column(0).GetValue<TimeOnly>(0)
        Assert.Equal(v_time, res_time)

        // TimeSpan
        let v_span = TimeSpan.FromHours(25.5)
        let res_span = df.Select(pl.lit v_span).Column(0).GetValue<TimeSpan>(0)
        Assert.Equal(v_span, res_span)

        // DateTimeOffset (带时区)
        let v_dto = DateTimeOffset(2023, 10, 1, 12, 0, 0, TimeSpan.FromHours(8.0))
        let res_dto = df.Select(pl.lit v_dto).Column(0).GetValue<DateTimeOffset>(0)
        // Polars 存储时区信息，取出来比较时应相等
        Assert.Equal(v_dto, res_dto)
    [<Fact>]
    member _.``Lit: Collections (Lists & Arrays)``() =
        // 准备一个只有 1 行的 DataFrame，方便观察 Lit 展开的效果
        // 如果 pl.lit([1; 2; 3]) 生效，select 的结果应该是 3 行
        let df = DataFrame.create [ Series.create("dummy", [0]) ]

        // ==========================================
        // 1. F# Lists (int list, string list...)
        // ==========================================
        
        // Integer List
        let listInt = [1; 2; 3]
        let dfInt = df.Select(pl.lit listInt)
        
        Assert.Equal(3L, dfInt.Height) // 长度应扩展为 3
        Assert.Equal(1, dfInt.Column(0).GetValue<int>(0))
        Assert.Equal(2, dfInt.Column(0).GetValue<int>(1))
        Assert.Equal(3, dfInt.Column(0).GetValue<int>(2))

        // String List
        let listStr = ["A"; "B"; "C"]
        let dfStr = df.Select(pl.lit listStr)
        
        Assert.Equal(3L, dfStr.Height)
        Assert.Equal("A", dfStr.Column(0).GetValue<string>(0))
        Assert.Equal("B", dfStr.Column(0).GetValue<string>(1))

        // ==========================================
        // 2. F# Arrays (int[], float[]...)
        // ==========================================

        // Double Array
        let arrF64 = [| 1.1; 2.2; 3.3; 4.4 |]
        let dfF64 = df.Select(pl.lit arrF64)

        Assert.Equal(4L, dfF64.Height)
        Assert.Equal(1.1, dfF64.Column(0).GetValue<double>(0))

        // Bool Array
        let arrBool = [| true; false |]
        let dfBool = df.Select(pl.lit arrBool)
        
        Assert.Equal(2L, dfBool.Height)
        Assert.True(dfBool.Column(0).GetValue<bool>(0))
        Assert.False(dfBool.Column(0).GetValue<bool>(1))

        // ==========================================
        // 3. Nullable Collections (Option List)
        // ==========================================

        // Int Option List (with None)
        let listOpt = [Some 10; None; Some 30]
        let dfOpt = df.Select(pl.lit listOpt)

        Assert.Equal(3L, dfOpt.Height)
        Assert.Equal(10, dfOpt.Column(0).GetValue<int>(0))
        Assert.True(dfOpt.Column(0).IsNullAt(1)) // Index 1 is Null
        Assert.Equal(30, dfOpt.Column(0).GetValue<int>(2))

        // String Option List
        let listStrOpt = [Some "Valid"; None]
        let dfStrOpt = df.Select(pl.lit listStrOpt)
        
        Assert.Equal(2L, dfStrOpt.Height)
        Assert.Equal("Valid", dfStrOpt.Column(0).GetValue<string>(0))
        Assert.True(dfStrOpt.Column(0).IsNullAt(1))

    [<Fact>]
    member _.``Lit: Empty Collections``() =
        let df = DataFrame.create [ Series.create("dummy", [0]) ]
        
        // Empty List -> Empty Series -> Empty Column
        let emptyList: int list = []
        let dfEmpty = df.Select(pl.lit emptyList)
        
        Assert.Equal(0L, dfEmpty.Height) // 结果应该是空表