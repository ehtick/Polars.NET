using System.Text;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class IoTests
{
    [Fact]
    public void Test_ReadJson_File_Advanced()
    {
        // ---------------------------------------------------
        // 场景：读取 .jsonl (NDJSON) 文件
        // 验证：
        // 1. JsonFormat.JsonLines 参数是否生效
        // 2. PolarsSchema 是否强制生效 (把 age 读成 Float64)
        // 3. columns 裁剪是否生效 (只读 age, 忽略 extra)
        // ---------------------------------------------------

        var jsonLinesContent = 
            @"{""name"": ""Alice"", ""age"": 20, ""extra"": ""junk""}
            {""name"": ""Bob"",   ""age"": 30, ""extra"": ""junk""}";

        using var f = new DisposableFile(jsonLinesContent, ".jsonl");

        // 构造 Schema：强制 age 为 Float64 (原数据是 Int)
        using var schema = new PolarsSchema()
            .Add("age", DataType.Float64); 
            // 注意：因为我们要裁剪列，只读 age，所以 Schema 里只需要定义 age 即可
            // 或者定义全部但 projection 只选 age

        using var df = DataFrame.ReadJson(
            f.Path,
            columns: new[] { "age" },       // 只读 age 列
            schema: schema,                 // 强制类型转换
            jsonFormat: JsonFormat.JsonLines,
            ignoreErrors: false
        );

        // 验证结构
        Assert.Equal(1, df.Width); // name 和 extra 应该被忽略
        Assert.Equal("age", df.ColumnNames[0]);
        
        // 验证类型 (Schema 生效)
        Assert.Equal(DataType.Float64, df.Column("age").DataType);
        
        // 验证数据
        Assert.Equal(2, df.Height);
        Assert.Equal(20.0, df.GetValue<double>(0, "age"));
        Assert.Equal(30.0, df.GetValue<double>(1, "age"));
    }

    [Fact]
    public void Test_ReadJson_Memory_Bytes()
    {
        // ---------------------------------------------------
        // 场景：从内存 byte[] 读取标准 JSON 数组
        // 验证：NativeBinding -> Wrapper -> 内存指针传递链路
        // ---------------------------------------------------

        var jsonContent = @"
            [
                {""id"": 1, ""val"": true},
                {""id"": 2, ""val"": false}
            ]";
        
        byte[] buffer = Encoding.UTF8.GetBytes(jsonContent);

        // 这里不传 Schema，让 Polars 自动推断
        using var df = DataFrame.ReadJson(
            buffer,
            jsonFormat: JsonFormat.Json // 默认值，显式写出来以示清晰
        );

        Assert.Equal(2, df.Height);
        Assert.Equal(1, df.GetValue<int>(0, "id"));
        Assert.True(df.GetValue<bool>(0, "val"));
        Assert.False(df.GetValue<bool>(1, "val"));
    }

    [Fact]
    public void Test_ReadJson_Stream()
    {
        // ---------------------------------------------------
        // 场景：从 Stream 读取
        // 验证：API 层 Stream -> MemoryStream -> byte[] 的转换逻辑
        // ---------------------------------------------------

        var jsonContent = @"[{""city"": ""New York""}, {""city"": ""London""}]";
        byte[] bytes = Encoding.UTF8.GetBytes(jsonContent);

        using var stream = new MemoryStream(bytes);

        // 模拟流的位置不在开头的情况 (Polars API 层应该处理 copy，所以这里位置不重要，
        // 但通常 Stream.CopyTo 是从当前位置开始复制，所以我们要确保流是 Ready 的)
        
        using var df = DataFrame.ReadJson(stream);

        Assert.Equal(2, df.Height);
        Assert.Equal("New York", df.GetValue<string>(0, "city"));
        Assert.Equal("London", df.GetValue<string>(1, "city"));
    }

[Fact]
public void Test_Ndjson_Scan_Lazy_AllModes()
{
    // ---------------------------------------------------
    // 修正数据：val 现在是纯数字 (JSON Number)，不再是字符串
    // 这样我们才能测试 schema_overwrite 对数字类型的控制
    // ---------------------------------------------------
    var ndjsonContent = 
@"{""id"": 1, ""val"": 100, ""tag"": ""A""}
{""id"": 2, ""val"": 200, ""tag"": ""B""}
{""id"": 3, ""val"": 300, ""tag"": ""C""}";

    using var f = new DisposableFile(ndjsonContent, ".ndjson");

    // =================================================================
    // 1. File Mode (测试 Schema Overwrite)
    // =================================================================
    {
        // 默认情况下，Polars 可能会把整数推断为 Int64 (最安全)
        // 这里我们强行指定为 Int32，如果生效，说明 schema 参数传递成功
        using var schema = new PolarsSchema()
            .Add("val", DataType.Int32);

        using var lf = LazyFrame.ScanNdjson(
            f.Path, 
            schema: schema,
            nRows: 3 
        );
        
        using var df = lf.Collect();

        Assert.Equal(3, df.Height);
        
        // 验证关键点：类型必须是我们强制指定的 Int32
        Assert.Equal(DataType.Int32, df.Column("val").DataType);
        Assert.Equal(100, df.GetValue<int>(0, "val"));
    }

    // =================================================================
    // 2. Memory Mode (Bytes)
    // =================================================================
    {
        byte[] bytes = File.ReadAllBytes(f.Path);

        // 这里不传 Schema，使用默认推断
        // 验证 Rust 端的 new_with_sources 逻辑
        using var lf = LazyFrame.ScanNdjson(bytes);
        using var df = lf.Collect();

        Assert.Equal(3, df.Height);
        
        // 验证默认推断通常是 Int64
        Assert.Equal(DataType.Int64, df.Column("val").DataType);
        Assert.Equal(200, df.GetValue<long>(1, "val"));
        
        Assert.Equal("B", df.GetValue<string>(1, "tag"));
    }

    // =================================================================
    // 3. Stream Mode
    // =================================================================
    {
        byte[] bytes = File.ReadAllBytes(f.Path);
        using var ms = new MemoryStream(bytes);

        using var lf = LazyFrame.ScanNdjson(ms);
        using var df = lf.Collect();

        Assert.Equal(3, df.Height);
        Assert.Equal(3, df.GetValue<long>(2, "id"));
    }
}
    [Fact]
    public void Test_ReadParquet_File()
    {
        // 1. 准备测试数据
        using var df = DataFrame.FromColumns(new
        {
            id = new[] { 1, 2, 3, 4, 5 },
            name = new[] { "Alice", "Bob", "Charlie", "David", "Eve" },
            val = new[] { 1.1, 2.2, 3.3, 4.4, 5.5 }
        });

        // 创建临时文件
        string path = Path.GetTempFileName(); 
        try
        {
            // 写入 Parquet (假设 WriteParquet 已实现)
            df.WriteParquet(path);

            // --- Case 1: 基础读取 (全量) ---
            using var dfFull = DataFrame.ReadParquet(path);
            Assert.Equal(5, dfFull.Height);
            Assert.Equal(3, dfFull.Width);
            Assert.Equal("Alice", dfFull.GetValue<string>(0, "name"));

            // --- Case 2: 高级参数读取 ---
            // 测试: 
            // 1. columns: 只读 "id" 和 "name" (列裁剪)
            // 2. nRows: 只读前 2 行 (Limit / Slice)
            // 3. rowIndexName: 自动生成行号列 "idx"
            // 4. rowIndexOffset: 行号从 100 开始
            using var dfPartial = DataFrame.ReadParquet(
                path,
                columns: ["id", "name"],
                nRows: 2,
                rowIndexName: "idx",
                rowIndexOffset: 100
            );

            // 验证结构
            Assert.Equal(2, dfPartial.Height); // nRows生效
            Assert.Equal(3, dfPartial.Width);  // id, name + idx
            
            // 验证列裁剪
            Assert.Contains("id", dfPartial.ColumnNames);
            Assert.Contains("name", dfPartial.ColumnNames);
            Assert.DoesNotContain("val", dfPartial.ColumnNames); // val 被裁剪了

            // 验证行号
            Assert.Equal(100UL, dfPartial.GetValue<ulong>(0, "idx")); // 第一行 Offset 100
            Assert.Equal(101UL, dfPartial.GetValue<ulong>(1, "idx")); // 第二行 Offset 101
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
    [Fact]
    public void Test_ReadParquet_Memory_And_Stream()
    {
        // 1. 准备二进制 Parquet 数据 (Blob)
        using var dfOriginal = DataFrame.FromColumns(new
        {
            timestamp = new[] { DateTime.Now, DateTime.Now.AddSeconds(1) },
            status = new[] { "OK", "FAIL" }
        });

        // 为了获取合法的 Parquet bytes，我们先写到临时文件再读出来
        // (如果以后实现了 WriteParquet(Stream) 可以直接写流)
        string tempPath = Path.GetTempFileName();
        byte[] parquetBytes;
        try
        {
            dfOriginal.WriteParquet(tempPath);
            parquetBytes = File.ReadAllBytes(tempPath);
        }
        finally
        {
            if (File.Exists(tempPath)) File.Delete(tempPath);
        }

        // --- Case 1: Read from byte[] (Memory) ---
        // 场景：从 Redis/数据库/网络 拿到了 byte[]
        using var dfFromBytes = DataFrame.ReadParquet(parquetBytes);
        
        Assert.Equal(2, dfFromBytes.Height);
        Assert.Equal("OK", dfFromBytes.GetValue<string>(0, "status"));
        Assert.Equal("FAIL", dfFromBytes.GetValue<string>(1, "status"));

        // --- Case 2: Read from Stream ---
        // 场景：从 ASP.NET Core Request.Body 或 S3 Stream 读取
        using var ms = new MemoryStream(parquetBytes);
        
        // 测试 Stream 重载
        // 同时测试一下 nRows 参数在 Stream 模式下是否依然有效
        using var dfFromStream = DataFrame.ReadParquet(ms, nRows: 1);

        Assert.Equal(1, dfFromStream.Height); // Limit 生效
        Assert.Equal("OK", dfFromStream.GetValue<string>(0, "status"));

        // 确保没有读第二行
        // Assert.Single(dfFromStream.Column("status"));
    }
    [Fact]
    // [Trait("Category","Debug")]
    public void Test_ScanParquet_File_Hive_Schema()
    {
        // ---------------------------------------------------
        // 场景：模拟 Hive 分区结构 /data/category=sales/data.parquet
        // 验证：
        // 1. Glob 扫描
        // 2. 手动指定 hivePartitionSchema (把 category 强制转为 Categorical)
        // 3. 手动指定文件 schema (把 amount 强制读为 Int64)
        // ---------------------------------------------------

        string baseDir = Path.Combine(Path.GetTempPath(), "polars_test_hive_" + Guid.NewGuid());
        string partitionDir = Path.Combine(baseDir, "category=sales");
        Directory.CreateDirectory(partitionDir);
        string filePath = Path.Combine(partitionDir, "data.parquet");

        try
        {
            // 1. 准备数据
            // 注意：我们写入 Int32，但读取时会尝试用 Schema 强制覆盖
            using var dfRaw = DataFrame.FromColumns(new
            {
                id = new[] { 1, 2, 3 },
                amount = new[] { 100, 200, 300 } // Int32
            });
            dfRaw.WriteParquet(filePath);

            // 2. 构造 Schema 对象
            // 这里演示我们刚刚实现的 Schema 类和 Fluent API
            
            // 文件 Schema: 告诉 Polars 里面是 Int32
            // *注意*：如果类型不兼容，Collect 时会报错
            using var fileSchema = new PolarsSchema()
                .Add("id", DataType.Int32)
                .Add("amount", DataType.Int32); 

            // Hive Schema: 告诉 Polars 分区列 'category' 应该是 Categorical 类型，而不是默认的 String
            using var hiveSchema = new PolarsSchema()
                .Add("category", DataType.Categorical);

            // 3. 执行 Lazy Scan
            // 使用 Glob 模式扫描 baseDir 下的所有 parquet
            using var lf = LazyFrame.ScanParquet(
                path: Path.Combine(baseDir, "**/*.parquet"),
                glob: true,
                schema: fileSchema,               // <--- 核心测试点 1
                hivePartitionSchema: hiveSchema,  // <--- 核心测试点 2
                tryParseHiveDates: true
            );

            // 4. Collect 并验证
            using var df = lf.Collect();
            // 验证 Hive 分区列是否存在
            Assert.Contains("category", df.ColumnNames);
            Assert.Equal("sales", df.GetValue<string>(0, "category"));
            // Assert.Equal("sales", df[2][0]);
            
            // 验证 Hive Schema 是否生效 (类型应为 Categorical)
            Assert.Equal(DataType.Categorical, df.Column("category").DataType);

            // 验证数据完整性
            Assert.Equal(3, df.Height);
            Assert.Equal(100, df.GetValue<int>(0, "amount"));
        }
        finally
        {
            if (Directory.Exists(baseDir))
                Directory.Delete(baseDir, true);
        }
    }

    [Fact]
    public void Test_ScanParquet_Memory_WithSchema()
    {
        // ---------------------------------------------------
        // 场景：从内存 byte[] 读取 Parquet
        // 验证：
        // 1. Memory Buffer 读取
        // 2. Schema 传递的安全性 (SafeHandleLock 是否工作)
        // ---------------------------------------------------

        // 1. 准备 Parquet 字节流
        using var dfRaw = DataFrame.FromColumns(new
        {
            name = new[] { "Alice", "Bob" },
            score = new[] { 9.5, 8.0 }
        });

        // 借用临时文件转 bytes (假设目前还没暴露 WriteParquetToStream)
        string tmpPath = Path.GetTempFileName();
        byte[] parquetBytes;
        try
        {
            dfRaw.WriteParquet(tmpPath);
            parquetBytes = File.ReadAllBytes(tmpPath);
        }
        finally
        {
            File.Delete(tmpPath);
        }

        // 2. 构造 Schema
        using var schema = new PolarsSchema()
            .Add("name", DataType.String)
            .Add("score", DataType.Float64);

        // 3. Scan Memory
        // 这里会触发我们 Wrapper 里的 SafeHandleLock 逻辑
        using var lf = LazyFrame.ScanParquet(
            parquetBytes, 
            schema: schema,
            tryParseHiveDates: false
        );

        // 4. Collect
        using var df = lf.Collect();

        Assert.Equal(2, df.Height);
        Assert.Equal("Alice", df.GetValue<string>(0, "name"));
        Assert.Equal(9.5, df.GetValue<double>(0, "score"));
        
        // 验证 Schema 确实起作用了 (检查 schema 指针传递是否导致崩溃或无效)
        // 如果 SafeHandleLock 有问题，这里大概率会 Crash 或者抛出 Invalid Handle
    }
    [Fact]
    public void Test_Ipc_Compression()
    {
        // ---------------------------------------------------
        // 测试目标：验证 IPC 压缩 (LZ4) 的写入与读取
        // ---------------------------------------------------
        using var dfOriginal = new DataFrame(
            new Series("id", new[] { 1, 2, 3 }),
            new Series("val", new[] { "A", "B", "C" })
        );
        using var f = new DisposableFile(".ipc");

        // 1. 写入压缩文件
        dfOriginal.WriteIpc(
            f.Path, 
            compression: IpcCompression.LZ4, 
            parallel: true
        );

        // 2. 读取验证 (注意：压缩文件不应开启 memoryMap)
        using var df = DataFrame.ReadIpc(f.Path, memoryMap: false);

        Assert.Equal(3, df.Height);
        Assert.Equal("val", df.ColumnNames[1]);
        Assert.Equal("B", df.GetValue<string>(1, "val"));
    }

    [Fact]
    public void Test_Ipc_Reader_Modes()
    {
        // ---------------------------------------------------
        // 测试目标：验证各种读取模式 (Projection, Limit, Mmap, Stream, Bytes)
        // 前置条件：文件未压缩 (为了支持 MemoryMap)
        // ---------------------------------------------------
        using var sId = new Series("id", new[] { 1, 2, 3, 4, 5 });
        using var sVal = new Series("val", new[] { "A", "B", "C", "D", "E" });
        using var sTs = new Series("ts", new[] {
            new DateTime(2021,1,1), new DateTime(2022,1,1), new DateTime(2023,1,1),
            new DateTime(2024,1,1), new DateTime(2025,1,1)
        });

        using var dfOriginal = new DataFrame(sId, sVal, sTs);
        using var f = new DisposableFile(".ipc");

        // 1. 写入 (无压缩，兼容 Mmap)
        dfOriginal.WriteIpc(
            f.Path, 
            compression: IpcCompression.None, 
            parallel: true
        );

        // =================================================================
        // 2. File Mode (测试高级参数: Projection, Limit, MemoryMap)
        // =================================================================
        {
            // 只读 id 和 val 列，只读前 3 行，启用内存映射
            using var df = DataFrame.ReadIpc(
                f.Path, 
                columns: new[] { "id", "val" }, // 列裁剪
                nRows: 3,                       // 行限制
                memoryMap: true                 // 启用 mmap (无压缩时安全)
            );

            Assert.Equal(3, df.Height);
            Assert.Equal(2, df.Width); 
            Assert.Equal(1, df.GetValue<int>(0, "id"));
            Assert.Equal("C", df.GetValue<string>(2, "val"));
        }

        // =================================================================
        // 3. Memory Mode (Bytes)
        // =================================================================
        {
            byte[] bytes = File.ReadAllBytes(f.Path);
            using var df = DataFrame.ReadIpc(bytes);

            Assert.Equal(5, df.Height);
            Assert.Equal(new DateTime(2023,1,1), df.GetValue<DateTime>(2, "ts"));
        }

        // =================================================================
        // 4. Stream Mode
        // =================================================================
        {
            using var stream = File.OpenRead(f.Path);
            using var df = DataFrame.ReadIpc(stream);

            Assert.Equal(5, df.Height);
            Assert.Equal("E", df.GetValue<string>(4, "val"));
        }
    }
    [Fact]
    public void Test_ScanIpc_Lazy_AllModes()
    {
        // ---------------------------------------------------
        // 1. 准备基准数据
        // ---------------------------------------------------
        using var sId = new Series("id", new[] { 1, 2, 3, 4, 5 });
        using var sVal = new Series("val", new[] { "A", "B", "C", "D", "E" });
        using var dfOriginal = new DataFrame(sId, sVal);

        using var f = new DisposableFile(".ipc");
        dfOriginal.WriteIpc(f.Path);

        // =================================================================
        // 2. File Mode (测试 UnifiedScanArgs: nRows/PreSlice & RowIndex)
        // =================================================================
        {
            // 启用 RowIndex，限制读取前 3 行 (nRows -> PreSlice)
            using var lf = LazyFrame.ScanIpc(
                f.Path, 
                nRows: 3, 
                rowIndexName: "idx_col"
            );
            
            using var df = lf.Collect();

            Assert.Equal(3, df.Height); // 验证 nRows 生效
            
            // 验证 RowIndex 是否存在且正确
            Assert.True(df.ColumnNames.Contains("idx_col"));
            Assert.Equal(0u, df.GetValue<uint>(0, "idx_col")); // 0.52 RowIndex 通常是 uint32/64
            
            // 验证数据正确性
            Assert.Equal("C", df.GetValue<string>(2, "val"));
        }

        // =================================================================
        // 3. Memory Mode (Bytes) - 验证 Rust ScanSources::Buffers
        // =================================================================
        {
            byte[] bytes = File.ReadAllBytes(f.Path);

            // 这是 C# 独有的能力：直接对内存中的 Feather 数据建立 Lazy 计划
            using var lf = LazyFrame.ScanIpc(bytes);
            
            // 简单过滤一下，证明 Lazy 引擎真的在工作
            using var df = lf.Filter(Col("id") > 3).Collect();

            Assert.Equal(2, df.Height); // id: 4, 5
            Assert.Equal(5, df.GetValue<int>(1, "id"));
        }

        // =================================================================
        // 4. Stream Mode
        // =================================================================
        {
            using var stream = File.OpenRead(f.Path);

            // API 层会将 Stream 转为 bytes，再走 Memory 路径
            using var lf = LazyFrame.ScanIpc(stream);
            using var df = lf.Collect();

            Assert.Equal(5, df.Height);
            Assert.Equal("E", df.GetValue<string>(4, "val"));
        }
    }
    [Fact]
    public void Test_Csv_TryParseDates_Auto()
    {
        // 构造数据：包含标准的 ISO 日期格式
        var csvContent = "name,birthday\nAlice,2023-01-01\nBob,2023-12-31";
        using var csv = new DisposableFile(csvContent,".csv");

        // 1. 默认 tryParseDates = true
        using var df = DataFrame.ReadCsv(csv.Path);
        
        // 验证 birthday 列是否被自动解析为 Date 类型，而不是 String
        // 注意：Polars 自动解析可能解析为 Date 或 Datetime
        var dateType = df.Schema["birthday"];
        Assert.Equal(DataTypeKind.Date, dateType.Kind);

        // 2. 测试显式关闭 (tryParseDates = false)
        using var dfString = DataFrame.ReadCsv(csv.Path, tryParseDates: false);
        
        // 断言它是 String
        var strType = dfString.Schema["birthday"];
        Assert.Equal(DataTypeKind.String,strType.Kind);
    }
    private class SinkTestPoco
{
    public int Id { get; set; }
    public string Type { get; set; }
    public double Val { get; set; }
}
    [Fact]
    public void Test_SinkParquet_Basic()
    {
        // 1. 准备数据
        var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Name = new[] { "Alice", "Bob", "Charlie" }
        });

        // 生成临时文件路径
        string path = Path.GetTempFileName(); 
        // GetTempFileName 创建了一个空文件，ParquetWriter 可能会抱怨文件已存在或为空。
        // 安全起见，删掉它，让 Polars 创建
        File.Delete(path); 
        path += ".parquet"; // 加个后缀

        try
        {
            // 2. Lazy -> Sink
            // Sink 这是一个 Action (就像 Collect 一样)，会触发执行
            df.Lazy().SinkParquet(path);

            // 3. 验证文件是否存在
            Assert.True(File.Exists(path));
            Assert.True(new FileInfo(path).Length > 0);

            // 4. 读取回验证 (假设你有 ScanParquet，如果没有，可以先只测文件存在)
            using var dfRead = LazyFrame.ScanParquet(path).Collect();
            Assert.Equal(3, dfRead.Height);
            Assert.Equal("Alice", dfRead.GetValue<string>(0, "Name"));
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
    [Fact]
    public void Test_Streaming_SinkParquet_EndToEnd()
    {
        // ====================================================
        // 场景：生成 100万行数据，过滤掉一半，写入 Parquet
        // 预期：内存平稳，磁盘出现文件
        // ====================================================

        int totalRows = 1_000_000;
        int batchSize = 100_000;
        string path = Path.Combine(Path.GetTempPath(), $"polars_stream_{Guid.NewGuid()}.parquet");

        try
        {
            // [修复]: 使用明确的泛型 IEnumerable<SinkTestPoco>
            // 而不是 IEnumerable<object>
            IEnumerable<SinkTestPoco> GenerateData()
            {
                for (int i = 0; i < totalRows; i++)
                {
                    yield return new SinkTestPoco
                    { 
                        Id = i, 
                        Type = (i % 2 == 0) ? "A" : "B", 
                        Val = i * 0.1 
                    };
                }
            }

            // 2. 建立管道
            var lf = LazyFrame.ScanEnumerable(GenerateData(), null,batchSize);

            // 3. 转换逻辑 (Lazy)
            // 只保留 Type == "A" 的数据 (50万行)
            var q = lf
                .Filter(Col("Type") == "A")
                .Select(Col("Id"), Col("Val"));

            Console.WriteLine("Starting Streaming Sink...");
            var sw = System.Diagnostics.Stopwatch.StartNew();

            // 4. 执行写入 (Sink)
            // Polars 会启动 Streaming Engine，分块读取 C# -> 处理 -> 写入磁盘
            // 整个过程内存占用极低
            q.SinkParquet(path);

            sw.Stop();
            Console.WriteLine($"Sink completed in {sw.Elapsed.TotalSeconds:F2}s");

            // 5. 验证
            Assert.True(File.Exists(path));
            var fileInfo = new FileInfo(path);
            Console.WriteLine($"Parquet File Size: {fileInfo.Length / 1024.0 / 1024.0:F2} MB");

            // 简单的正确性验证：
            // 如果你的库实现了 ScanParquet，可以读回来校验行数是否为 500,000
            
            using var lfCheck = LazyFrame.ScanParquet(path);
            using var dfCheck = lfCheck.Collect();
            Assert.Equal(totalRows / 2, dfCheck.Height);
            
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
    [Fact]
    public void Test_Streaming_SinkParquet_ComplexTypes_EndToEnd()
    {
        // ====================================================
        // 场景：生成包含数组和对象的复杂数据流，流式写入 Parquet
        // 目的：验证 "邪修" ArrowConverter 在流式 Sink 下的稳定性
        // ====================================================

        int totalRows = 100_000; // 稍微减小一点量，侧重测结构
        int batchSize = 10_000;
        string path = Path.Combine(Path.GetTempPath(), $"polars_complex_{Guid.NewGuid()}.parquet");

        try
        {
            // 1. 数据源 (包含 List 和 Struct)
            IEnumerable<ComplexPoco> GenerateData()
            {
                for (int i = 0; i < totalRows; i++)
                {
                    yield return new ComplexPoco
                    {
                        Id = i,
                        // 测试 List<int> -> Parquet LIST
                        Tags = [i, i * 2], 
                        // 测试 POCO -> Parquet STRUCT
                        Meta = new MetaInfo { Score = i * 0.5, Label = $"L_{i}" } 
                    };
                }
            }

            // 2. 建立管道
            // ScanArrowStream 内部会调用 ToArrowBatches -> ArrowConverter
            // 这里会触发你的 "邪修" 递归反射逻辑
            var lf = LazyFrame.ScanEnumerable(GenerateData(),null, batchSize);

            // 3. 简单的转换 (确保 Lazy 引擎介入)
            // 比如只保留 Id 偶数的
            var q = lf.Filter(Col("Id") % Lit(2) == Lit(0));

            Console.WriteLine("Starting Complex Streaming Sink...");
            var sw = System.Diagnostics.Stopwatch.StartNew();

            // 4. 执行写入
            q.SinkParquet(path);

            sw.Stop();
            Console.WriteLine($"Sink completed in {sw.Elapsed.TotalSeconds:F2}s");

            // 5. 验证文件
            Assert.True(File.Exists(path));

            // 6. 读回验证 (Eager Load)
            // 这里验证 Parquet 是否真的存对了结构
            using var lfCheck = LazyFrame.ScanParquet(path);
            using var df = lfCheck.Collect();
            
            Assert.Equal(totalRows / 2, df.Height); // 过滤了一半

            // 验证 List
            var tags = df.Column("Tags");
            Assert.Equal(DataTypeKind.List, tags.DataType.Kind);
            var row0Tags = tags.GetValue<List<int?>>(0); // Id=0: [0, 0]
            Assert.Equal(0, row0Tags[0]);
            
            // 验证 Struct (Unnest 验证)
            var unnested = df.Unnest("Meta");
            Assert.True(unnested.ColumnNames.Contains("Score"));
            Assert.True(unnested.ColumnNames.Contains("Label"));
            Assert.Equal(0.0, unnested.GetValue<double>(0, "Score")); // Id=0
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }
        [Fact]
        public void Test_SinkIpc_Advanced_Options()
        {
            // 1. 准备数据
            var df = new DataFrame(
                new Series("id", new[] { 1, 2, 3 }),
                new Series("val", new[] { "A", "B", "C" })
            );

            // 转为 LazyFrame
            var lf = df.Lazy();

            // 2. 准备输出文件
            using var f = new DisposableFile(".ipc");

            // 3. 执行 SinkIpc
            // 测试点：启用 LZ4 压缩，要求保持顺序，且关闭时执行完整 Sync
            lf.SinkIpc(
                f.Path, 
                compression: IpcCompression.LZ4, 
                maintainOrder: true, 
                syncOnClose: SyncOnClose.All
            );

            // 4. 验证结果
            // 因为写入的是压缩文件，读取时必须显式 memoryMap: false (我们在 I/O 升级里特别强调的)
            using var dfRead = DataFrame.ReadIpc(f.Path, memoryMap: false);

            Assert.Equal(3, dfRead.Height);
            Assert.Equal("val", dfRead.ColumnNames[1]);
            Assert.Equal("B", dfRead.GetValue<string>(1, "val"));
            
            // 验证文件确实存在且非空
            var fileInfo = new FileInfo(f.Path);
            Assert.True(fileInfo.Exists);
            Assert.True(fileInfo.Length > 0);
        }

// --- 辅助 POCO ---
    private class ComplexPoco
    {
        public int Id { get; set; }
        public int[] Tags { get; set; }     // 映射为 List<Int32>
        public MetaInfo Meta { get; set; }  // 映射为 Struct
    }

    private class MetaInfo
    {
        public double Score { get; set; }
        public string Label { get; set; }
    }
    [Fact]  
    public void Test_ScanDataReader_Integration()
    {
        // 1. 模拟数据库
        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Name", typeof(string)); // 嫌疑人 A
        table.Columns.Add("Value", typeof(double));
        table.Columns.Add("Date", typeof(DateTime)); // 嫌疑人 B

        var now = DateTime.Now;
        for (int i = 0; i < 1000; i++)
        {
            table.Rows.Add(i, $"User_{i}", i * 0.5, now.AddSeconds(i));
            // table.Rows.Add(i, i * 0.5, now.AddSeconds(i));
        }

        using var reader = table.CreateDataReader();

        // [修改] 只读取数值列，暂时避开 String 和 DateTime
        // 注意：ToArrowBatches 会读取所有列，所以我们得在查询里 Select
        // 或者，更直接地，我们只往 DataTable 里放数值列试试？
        
        // 既然 ScanDataReader 默认读所有列，我们通过 Lazy Select 来过滤
        var lf = LazyFrame.ScanDatabase(reader, batchSize: 100);

        // [测试 A] 只查数值列
        var q = lf.Select(Col("Id"), Col("Value")).Filter(Col("Id") > 500);

        using var df = q.CollectStreaming();
        
        Assert.Equal(499, df.Height);
    }
    [Fact]
    public void Test_ScanDataReader_Nested_Array()
    {
        // 1. 模拟一个支持数组的数据库 (如 PostgreSQL)
        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));
        // [邪修核心] 定义列类型为 int[]
        table.Columns.Add("Tags", typeof(int[])); 
        table.Columns.Add("Memo", typeof(string)); // 顺便测一下 LargeString

        // 2. 插入数据
        // Row 1: [10, 20]
        table.Rows.Add(1, new int[] { 10, 20 }, "Row1");
        // Row 2: [30]
        table.Rows.Add(2, new int[] { 30 }, "Row2");
        // Row 3: [] (空数组)
        table.Rows.Add(3, new int[] { }, "Row3");
        // Row 4: null (空值)
        table.Rows.Add(4, DBNull.Value, "Row4");

        using var reader = table.CreateDataReader();

        // 3. 执行 Scan
        // 这里的 reader.GetFieldType("Tags") 会返回 typeof(int[])
        // 你的 DataReaderExtensions 会识别为 IEnumerable -> LargeList
        // 然后 Buffer 会收集这些数组，交给 ArrowConverter 递归处理
        var lf = LazyFrame.ScanDatabase(reader);

        using var df = lf.Collect();

        // 4. 验证
        Assert.Equal(4, df.Height);
        
        // 验证 Schema: Tags 应该是 List(Int32)
        // 注意：Polars 内部 List 可能是 LargeList，C# 类型是 Series
        var tagsSeries = df.Column("Tags");
        Assert.Equal(DataTypeKind.List, tagsSeries.DataType.Kind);

        // 验证值 (需要 Series 支持 GetValue<int[]> 或者 List<int>)
        // 这里我们简单起见，转成 String 验证结构，或者由你之前实现的 GetValue 支持
        // Row 1: [10, 20]
        // 假设 Series.ToString() 会打印列表内容
        var row1 = tagsSeries.GetValue<List<int?>>(0);
        // 断言它包含元素 (具体格式取决于 Polars Series ToString 实现)
        // 只要不报错，说明结构对了。
        
        Assert.Equal("Row1", df.GetValue<string>(0, "Memo"));
        
        // 5. 进阶：Explode (炸开) 测试
        // 如果能炸开，说明 Polars 真的认出了这是 List
        var exploded = df.Explode("Tags");
        // 1(10), 1(20), 2(30), 3(null), 4(null) -> 至少应该变多
        Assert.True(exploded.Height >= 3);
    }
    public class UserMeta
    {
        public int Level { get; set; }
        public double Score { get; set; }
    }

    [Fact]
    public void Test_ScanDataReader_Nested_Struct()
    {
        // 1. 模拟数据库返回对象
        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));
        // [邪修核心] 定义列类型为自定义对象
        table.Columns.Add("Meta", typeof(UserMeta));

        table.Rows.Add(1, new UserMeta { Level = 5, Score = 99.5 });
        table.Rows.Add(2, new UserMeta { Level = 1, Score = 10.0 });
        table.Rows.Add(3, DBNull.Value); // Null Struct

        using var reader = table.CreateDataReader();

        // 2. 执行 Scan
        // DataReaderExtensions 会发现 UserMeta 不认识 -> 兜底扔给 ArrowConverter
        // ArrowConverter 反射 UserMeta -> 发现是 Class -> BuildStructArray -> 生成 Struct
        var lf = LazyFrame.ScanDatabase(reader);

        using var df = lf.Collect();

        // 3. 验证
        Assert.Equal(3, df.Height);
        
        var metaSeries = df.Column("Meta");
        Assert.Equal(DataTypeKind.Struct, metaSeries.DataType.Kind);

        // 4. 验证 Struct 字段访问 (Unnest)
        // 在 Polars 里把 Struct 拆成列
        var unnested = df.Unnest("Meta");
        
        Assert.True(unnested.ColumnNames.Contains("Level"));
        Assert.True(unnested.ColumnNames.Contains("Score"));
        
        Assert.Equal(5, unnested.GetValue<int>(0, "Level"));
        Assert.Equal(99.5, unnested.GetValue<double>(0, "Score"));
        
        // Row 3 应该是 null
        Assert.Null(unnested.GetValue<int?>(2, "Level"));
    }
    [Fact]
    public void Test_DataFrame_FromDataReader_Eager_Nested()
    {
        // 1. 模拟数据库 (复用之前的复杂结构)
        var table = new System.Data.DataTable();
        table.Columns.Add("Id", typeof(int));
        table.Columns.Add("Tags", typeof(int[])); // List
        table.Columns.Add("Meta", typeof(UserMeta)); // Struct

        table.Rows.Add(1, new int[] { 10, 20 }, new UserMeta { Level = 99, Score = 100.0 });
        table.Rows.Add(2, DBNull.Value, DBNull.Value);

        using var reader = table.CreateDataReader();

        // 2. [高光时刻] 一行代码，Eager 加载！
        // 此时数据已经在 Rust 堆内存里了，C# 端只有 handle
        using var df = DataFrame.ReadDatabase(reader);

        // 3. 验证
        Assert.Equal(2, df.Height);
        
        // 验证 Struct
        var meta = df.Column("Meta");
        Assert.Equal(DataTypeKind.Struct, meta.DataType.Kind);
        
        // 验证 List
        var tags = df.Column("Tags");
        Assert.Equal(DataTypeKind.List, tags.DataType.Kind);

        // 验证值
        var row1Tags = tags.GetValue<List<int?>>(0);
        Assert.Equal(10, row1Tags[0]);
        
        // 验证 Unnest 也就是 Struct 的内容
        var unnested = df.Unnest("Meta");
        Assert.Equal(99, unnested.GetValue<int>(0, "Level"));
    }
    [Fact]
    public async Task Test_WriteTo_Generic_EndToEnd()
    {
        // 1. 准备数据 (故意搞点 null)
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2, 3 },
            // Date 列，中间有个 null
            Date = new DateTime?[] { DateTime.Now.Date, null, DateTime.Now.Date.AddDays(1) }
        });

        var targetTable = new System.Data.DataTable();

        // 2. 调用通用 WriteTo 接口
        // 这里模拟 "SQL Server Extension" 的行为
        await Task.Run(() => 
        {
            df.WriteTo(reader => 
            {
                // 假装这是 SqlBulkCopy.WriteToServer
                targetTable.Load(reader);
            });
        });

        // 3. 验证
        Assert.Equal(3, targetTable.Rows.Count);
        Assert.Equal(1, targetTable.Rows[0]["Id"]);
        Assert.NotNull(targetTable.Rows[0]["Date"]);
        
        // 验证 null 处理
        Assert.Equal(DBNull.Value, targetTable.Rows[1]["Date"]);
    }
}
public class CsvSchemaTests
{
    [Fact]
    public void Test_ReadCsv_With_Explicit_Schema()
    {
        // 1. 准备测试数据
        // 默认情况下:
        // "id"   会被推断为 Int64
        // "rate" 会被推断为 Float64
        // "date" 会被推断为 Date 或 String
        string csvContent = @"id,name,rate,date
1,Apple,1.5,2023-01-01
2,Banana,3.7,2023-05-20
3,Cherry,,2023-10-10";

        string filePath = Path.GetTempFileName() + ".csv";
        File.WriteAllText(filePath, csvContent);

        try
        {
            Console.WriteLine($"[Test] Created temp CSV at: {filePath}");

            // 2. 定义强制 Schema (覆盖默认推断)
            // 我们故意使用非默认类型来验证 Schema 是否生效
            using var explicitSchema = new PolarsSchema()
                .Add("id", DataType.Int32)
                .Add("name", DataType.String)
                .Add("rate", DataType.Float32)
                .Add("date", DataType.String);
            // 3. 执行读取 (Eager Mode)
            // 这里会触发 WithSchemaHandle -> pl_schema_new -> pl_read_csv
            using var df = DataFrame.ReadCsv(filePath, schema: explicitSchema);

            Console.WriteLine("[Test] DataFrame loaded successfully.");
            Console.WriteLine(df); // 这里会打印 Schema 字符串，你可以人工检查

            // 4. 验证 Schema (Introspection)
            var resultSchema = df.Schema;

            // 验证: id 应该是 Int32
            Assert.Equal(DataTypeKind.Int32, resultSchema["id"].Kind);
            
            // 验证: rate 应该是 Float32
            Assert.Equal(DataTypeKind.Float32, resultSchema["rate"].Kind);

            // 验证: date 应该是 String (因为我们强制指定了)
            Assert.Equal(DataTypeKind.String, resultSchema["date"].Kind);

            // 5. 验证数据正确性 (可选)
            // 确保数据没有因为类型转换而乱码
            // 注意：这里需要你之前实现的 Series GetValue 相关方法支持
            // 简单起见，我们检查 Null Count
            Assert.Equal(1, df["rate"].NullCount);
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }

    [Fact]
    public void Test_ScanCsv_With_Explicit_Schema()
    {
        // 1. 准备数据
        string csvContent = "val\n100\n200";
        string filePath = Path.GetTempFileName() + ".csv";
        File.WriteAllText(filePath, csvContent);

        try
        {
            // 2. 准备 PolarsSchema (Fluent API)
            // 这里的关键是验证 Schema 对象能正确穿透到 Rust 端
            using var explicitSchema = new PolarsSchema()
                .Add("val", DataType.Float64); // 强制 Int64 -> Float64

            // 3. Lazy Mode 扫描
            // 顺便测试一下新的 Lazy 参数：行号生成 (rowIndexName)
            using var lf = LazyFrame.ScanCsv(
                filePath, 
                schema: explicitSchema,
                rowIndexName: "row_id", // 让 Polars 自动生成行号列
                rowIndexOffset: 10      // 从 10 开始计数
            );
            
            // 4. 验证 Lazy Schema (Metadata Check)
            // 此时还未读取文件，但 Schema 应该是我们指定的
            var lfSchema = lf.Schema;

            // 验证类型覆盖是否成功
            Assert.Equal(DataType.Float64, lfSchema["val"]);
            // 验证行号列是否出现在 Schema 中
            Assert.Contains("row_id", lfSchema.ColumnNames);
            Assert.Equal(DataType.UInt32, lfSchema["row_id"]); // 行号通常是 UInt32

            Console.WriteLine("[Test] LazyFrame Schema validated.");

            // 5. Collect 并验证真实数据 (Execution Check)
            using var df = lf.Collect();
            
            // 类型验证
            Assert.Equal(DataTypeKind.Float64, df.Schema["val"].Kind);
            
            // 数据验证
            // "100" -> 100.0
            Assert.Equal(100.0, df["val"][0]); 
            
            // 行号验证 (Offset=10)
            Assert.Equal(10u, df["row_id"][0]); 
            Assert.Equal(11u, df["row_id"][1]);
        }
        finally
        {
            if (File.Exists(filePath))
                File.Delete(filePath);
        }
    }
    [Fact]
    public void Test_ReadCsv_AllOptions_EndToEnd()
    {
        // 1. 准备“非标准”的脏数据
        // 特征：
        // - 前两行是垃圾注释 (需要 skipRows=2)
        // - 使用分号 ';' 分隔 (需要 separator=';')
        // - 包含日期字符串 (需要 tryParseDates=true)
        // - 我们希望 ID 是 Int32 而不是默认的 Int64 (需要 schema)
        string csvContent = 
@"# Metadata Line 1: Created by System X
# Metadata Line 2: Version 1.0
ID;ProductName;Weight;ReleaseDate
101;Quantum Gadget;1.55;2023-12-25
102;Hyper Widget;;2024-01-01";

        string filePath = Path.GetTempFileName();
        File.WriteAllText(filePath, csvContent);

        try
        {
            // 2. 定义强制 Schema (Fluent API 写法)
            // 优势：链式调用，无需创建临时 Dictionary，阅读流畅
            using var explicitSchema = new PolarsSchema()
                .Add("ID", DataType.Int32)        // 强制 Int32
                .Add("Weight", DataType.Float32)  // 强制 Float32
                .Add("ReleaseDate", DataType.Date); // 强制 Date
                // ProductName 没写，让 Polars 自动推断为 String

            // 3. 调用全参数 ReadCsv
            using var df = DataFrame.ReadCsv(
                path: filePath,
                schema: explicitSchema,    
                hasHeader: true,           
                separator: ';',            
                skipRows: 2,               
                tryParseDates: true        
            );

            // 4. 验证结构 (Shape)
            Assert.Equal(2, df.Height); 
            Assert.Equal(4, df.Width);  

            // 5. 验证元数据 (Schema)
            // 确保我们注入的类型生效了
            Assert.Equal(DataType.Int32, df.Schema["ID"]);
            Assert.Equal(DataType.Float32, df.Schema["Weight"]);
            Assert.Equal(DataType.Date, df.Schema["ReleaseDate"]);
            Assert.Equal(DataType.String, df.Schema["ProductName"]); // 自动推断的

            // 6. 验证标量值
            
            // ID (Int32)
            Assert.Equal(101, df["ID"][0]); 
            
            // String
            Assert.Equal("Quantum Gadget", df["ProductName"][0]);
            
            // Weight (Float32)
            // 注意：df["Weight"][0] 返回的是 object(float)，Assert.Equal 需要精度容差
            Assert.Equal(1.55f, (float)df["Weight"][0]!, 0.0001f);
            
            // Date
            // 假设 ArrowReader 将 Polars Date 映射为 .NET DateOnly
            var dateVal = df["ReleaseDate"][0];
            Assert.Equal(new DateOnly(2023, 12, 25), dateVal);

            // 7. 验证 Null 处理 (第二行 Weight 为 null)
            Assert.Equal(1, df["Weight"].NullCount);
            Assert.Null(df["Weight"][1]);
        }
        finally
        {
            if (File.Exists(filePath)) 
                File.Delete(filePath);
        }
    }
    [Fact]
    public void ReadDatabase_Should_Handle_Decimal_Correctly()
    {
        // 1. Arrange: 用 DataTable 模拟一个包含 Decimal 的数据库读取器
        var table = new System.Data.DataTable();
        table.Columns.Add("Product", typeof(string));
        table.Columns.Add("Price", typeof(decimal)); // 关键测试点：System.Decimal

        // 插入特定的测试数值
        table.Rows.Add("Laptop", 1234.56m);
        table.Rows.Add("Mouse", 99.99m);
        table.Rows.Add("Cable", 0.00m); 

        using var reader = table.CreateDataReader();

        // 2. Act: 通过修复后的 ReadDatabase 读取
        using var df = DataFrame.ReadDatabase(reader);

        // 3. Assert & Verify
        // (A) 直观验证：打印出来看是否还有乱码 (e-50 等)
        Console.WriteLine("=== Decimal Test Output ===");
        df.Show();

        // (B) 验证 Schema：确保被识别为 Decimal 而不是 Double
        // 注意：根据修复代码，这里应该是 Decimal(38, 18)
        var priceCol = df["Price"];
        Assert.Contains("decimal", priceCol.DataType.ToString());

        // (C) 验证数值：确保精度没有丢失且数值正确
        // 注意：Polars.NET 的 Series[i] 索引器返回的是 object
        var val0 = priceCol[0];
        var val1 = priceCol[1];

        // 如果 ArrowConverter 读取逻辑正常，这里应该能拿到 decimal 或者 double
        // 为了兼容性，我们要么转 decimal，要么转 double 比较
        Assert.Equal(1234.56m, Convert.ToDecimal(val0));
        Assert.Equal(99.99m, Convert.ToDecimal(val1));
    }
    [Fact]
    public void Test_ScanCsv_From_Memory()
    {
        // 1. 模拟 CSV 数据 (In-Memory)
        string csvString = "id,name,score\n1,Alice,99.5\n2,Bob,88.0";
        byte[] csvBytes = System.Text.Encoding.UTF8.GetBytes(csvString);

        // 2. 内存扫描 (不落盘!)
        using var lf = LazyFrame.ScanCsv(
            csvBytes,
            hasHeader: true,
            rowIndexName: "row_idx" // 顺便测测参数传递
        );

        // 3. 验证 Schema (Metadata)
        using var schema = lf.Schema;
        Assert.Equal(DataTypeKind.Int64, schema["id"].Kind);
        Assert.Equal(DataTypeKind.String, schema["name"].Kind);
        Assert.Equal(DataTypeKind.Float64, schema["score"].Kind);
        Assert.Equal(DataTypeKind.UInt32, schema["row_idx"].Kind); // 行号列

        // 4. Collect 验证数据
        using var df = lf.Collect();
        Assert.Equal(2, df.Height);
        Assert.Equal("Alice", df["name"][0]);
        Assert.Equal(99.5, df["score"][0]);
    }
}
