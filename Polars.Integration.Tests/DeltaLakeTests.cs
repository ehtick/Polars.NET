using Polars.Integration.Tests.Fixtures;
using Polars.CSharp;
using System.Text;
using Minio;
using Minio.DataModel.Args;
using Polars.Integration.Tests.Utils;
using Polars.NET.Core;

namespace Polars.Integration.Tests;

public class DeltaLakeTests : IClassFixture<MinioFixture>
{
    private readonly MinioFixture _minio;

    public DeltaLakeTests(MinioFixture minio)
    {
        _minio = minio;
    }

    [Fact]
    public async Task Test_Scan_Delta_AWS_Minio()
    {
        // ==========================================
        // 1. 准备环境 & 配置
        // ==========================================
        var tableName = $"delta_table_{Guid.NewGuid()}";
        var rootUrl = $"s3://{_minio.BucketName}/{tableName}";
        var parquetFileName = "part-0000.parquet";
        var parquetUrl = $"{rootUrl}/{parquetFileName}";

        // ==========================================
        // 关键修复：处理 Endpoint 格式
        // ==========================================
        // 1. MinIO 的 Endpoint 通常是 "127.0.0.1:32899" 这种格式
        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');

        // 2. Polars (Rust) 必须要有协议头，否则 Url::parse 失败 -> 回退到真实 AWS
        var polarsEndpoint = $"http://{rawEndpoint}";

        // 3. MinIO .NET SDK 必须没有协议头，否则报错 InvalidEndpoint
        var minioSdkEndpoint = rawEndpoint;

        // 构造 CloudOptions 给 Polars
        var options = CloudOptions.Aws(
            region: _minio.Region,
            accessKey: _minio.AccessKey,
            secretKey: _minio.SecretKey,
            endpoint: polarsEndpoint // <--- 传入带 http:// 的地址
        );
        options.Credentials!["aws_allow_http"] = "true"; // 允许 HTTP
        options.Credentials!["aws_s3_force_path_style"] = "true"; // 强制 Path Style

        // ==========================================
        // 2. 第一步：使用 Polars 写入 Parquet 数据文件
        //    (假装它是 Delta Table 的一部分)
        // ==========================================
        using var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3 },
            Name = new[] { "Polars", "Delta", "Rust" },
            Value = new[] { 10.5, 20.0, 30.5 }
        });

        df.Lazy().SinkParquet(parquetUrl, cloudOptions: options);

        // ==========================================
        // 3. 第二步：手动构造 Delta Log (修正字段名大小写)
        // ==========================================
        
        // 1. 准备 Schema 字符串 (这部分保持不变)
        var schemaObj = new
        {
            type = "struct",
            fields = new object[]
            {
                new { name = "Id", type = "integer", nullable = true, metadata = new { } },
                new { name = "Name", type = "string", nullable = true, metadata = new { } },
                new { name = "Value", type = "double", nullable = true, metadata = new { } }
            }
        };
        var schemaString = System.Text.Json.JsonSerializer.Serialize(schemaObj);

        var sb = new StringBuilder();

        // 2. Line 1: Protocol (全小写)
        // 格式: {"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
        sb.AppendLine("{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}");

        // 3. Line 2: Metadata (关键修正: metaData，D大写！)
        // 我们手动构建外层 JSON，内层用 Serialize 处理复杂结构
        var metadataInner = new
        {
            id = Guid.NewGuid().ToString(),
            format = new { provider = "parquet", options = new { } },
            schemaString = schemaString,
            partitionColumns = new string[0],
            configuration = new { },
            createdTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
        // 修正点：这里手动拼接 "metaData": ...
        sb.AppendLine($"{{\"metaData\":{System.Text.Json.JsonSerializer.Serialize(metadataInner)}}}");

        // 4. Line 3: Add (全小写)
        var addInner = new
        {
            path = parquetFileName,
            partitionValues = new { },
            size = 1000L,
            modificationTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            dataChange = true,
            stats = "{\"numRecords\":3}"
        };
        // 修正点：这里手动拼接 "add": ...
        sb.AppendLine($"{{\"add\":{System.Text.Json.JsonSerializer.Serialize(addInner)}}}");

        var logContent = sb.ToString();
        
        // 调试用：打印生成的 JSON 看看长啥样
        // Console.WriteLine("Generated Delta Log:\n" + logContent);

        var logPath = $"{tableName}/_delta_log/00000000000000000000.json";

        // 使用 MinioClient 直接上传 Log 文件
        // 注意：这里需要依赖 Minio NuGet 包
        var minioClient = new MinioClient()
            .WithEndpoint(minioSdkEndpoint)
            .WithCredentials(_minio.AccessKey, _minio.SecretKey)
            .WithRegion(_minio.Region)
            .Build();

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(logContent));
        await minioClient.PutObjectAsync(new PutObjectArgs()
            .WithBucket(_minio.BucketName)
            .WithObject(logPath)
            .WithStreamData(stream)
            .WithObjectSize(stream.Length)
            .WithContentType("application/json"));

        // ==========================================
        // 4. 第三步：Scan Delta (测试核心目标)
        // ==========================================
        // 这一步会触发 Rust 端的 pl_scan_delta -> deltalake::open_table -> Polars Scan
        using var lfRead = LazyFrame.ScanDelta(rootUrl, cloudOptions: options);
        using var dfRead = lfRead.Collect();
        Console.WriteLine("DataFrame From Delta Table Here:");
        df.Show();
        // ==========================================
        // 5. 验证
        // ==========================================
        Assert.Equal(df.Height, dfRead.Height);
        
        var originalNames = df["Name"].ToArray<string>();
        var readNames = dfRead["Name"].ToArray<string>();
        
        Assert.Equal(originalNames, readNames);
        
        // 验证是否正确使用了 Delta Log 中的 Schema (例如列名)
        Assert.Contains("Value", dfRead.ColumnNames);
        
        Console.WriteLine($"Delta Lake Scan (S3/MinIO) Passed! Table: {tableName}");
    }
    [Fact]
    [Trait("DeltaLake","TimeTravel")]
    public async Task Test_Scan_Delta_TimeTravel()
    {
        // ==========================================
        // 1. 环境准备
        // ==========================================
        var tableName = $"delta_time_travel_{Guid.NewGuid()}";
        var rootUrl = $"s3://{_minio.BucketName}/{tableName}";
        
        // 构造 Endpoint
        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}"; // Polars 需要 http://
        
        // Polars Cloud Options
        var options = CloudOptions.Aws(
            region: _minio.Region,
            accessKey: _minio.AccessKey,
            secretKey: _minio.SecretKey,
            endpoint: polarsEndpoint
        );
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";

        // MinIO Client (用于上传 Log)
        var minioClient = new MinioClient()
            .WithEndpoint(rawEndpoint)
            .WithCredentials(_minio.AccessKey, _minio.SecretKey)
            .WithRegion(_minio.Region)
            .Build();

        // 定义时间点 (用于 Datetime Travel 测试)
        // V0 创建于 1小时前
        var timeV0 = DateTimeOffset.UtcNow.AddHours(-1); 
        // V1 创建于 现在
        var timeV1 = DateTimeOffset.UtcNow;

        // ==========================================
        // 2. 构造 Version 0 (Initial Commit)
        // ==========================================
        var fileV0 = "part-v0.parquet";
        
        // 2.1 写入 V0 数据 Parquet
        using (var df0 = DataFrame.FromColumns(new { 
            Id = new[] { 1 }, Name = new[] { "V0" }, Value = new[] { 10.0 } 
        }))
        {
            df0.Lazy().SinkParquet($"{rootUrl}/{fileV0}", cloudOptions: options);
        }

        // 2.2 生成 Log V0 (Protocol + Metadata + Add)
        var sb0 = new StringBuilder();
        sb0.AppendLine(DeltaLakeTestHelper.ActionProtocol());
        sb0.AppendLine(DeltaLakeTestHelper.ActionMetadata(
            DeltaLakeTestHelper.GenerateSchemaString(), 
            timeV0.ToUnixTimeMilliseconds() // 手动指定时间
        ));
        sb0.AppendLine(DeltaLakeTestHelper.ActionAdd(fileV0, modTime: timeV0.ToUnixTimeMilliseconds()));
        
        await DeltaLakeTestHelper.UploadLogAsync(minioClient, _minio.BucketName, tableName, 0, sb0.ToString());

        // ==========================================
        // 3. 构造 Version 1 (Append Commit)
        // ==========================================
        var fileV1 = "part-v1.parquet";

        // 3.1 写入 V1 数据 Parquet
        using (var df1 = DataFrame.FromColumns(new { 
            Id = new[] { 2 }, Name = new[] { "V1" }, Value = new[] { 20.0 } 
        }))
        {
            df1.Lazy().SinkParquet($"{rootUrl}/{fileV1}", cloudOptions: options);
        }

        // 3.2 生成 Log V1 (只有 Add，继承之前的 Protocol/Metadata)
        // 注意：标准的 Delta Log，后续版本通常只包含 Add/Remove 动作，
        // 除非 Metadata 变了，否则不需要再写 Protocol/Metadata
        var sb1 = new StringBuilder();
        sb1.AppendLine(DeltaLakeTestHelper.ActionAdd(fileV1, modTime: timeV1.ToUnixTimeMilliseconds()));

        await DeltaLakeTestHelper.UploadLogAsync(minioClient, _minio.BucketName, tableName, 1, sb1.ToString());

        // ==========================================
        // 4. 测试 A: 读取最新版 (Snapshot / Version 1)
        // ==========================================
        using var lfLatest = LazyFrame.ScanDelta(rootUrl, cloudOptions: options);
        var dfLatest = lfLatest.Collect();
        
        Assert.Equal(2, dfLatest.Height); // 应该有 1 和 2 两行
        Assert.Contains("V1", dfLatest["Name"].ToArray<string>());
        Console.WriteLine("Test A (Latest) Passed");

        // ==========================================
        // 5. 测试 B: Time Travel via Version (回到 V0)
        // ==========================================
        using var lfV0 = LazyFrame.ScanDelta(rootUrl, version: 0, cloudOptions: options);
        var dfV0 = lfV0.Collect();

        Assert.Equal(1, dfV0.Height); // 只有 V0 那一行
        Assert.Equal("V0", dfV0["Name"].ToArray<string>()[0]);
        Console.WriteLine("Test B (Version Travel) Passed");

        // ==========================================
        // 6. 测试 C: Time Travel via Datetime
        // ==========================================
        // 我们查询 timeV0 之后 1 分钟的时间点 (那时 V1 还没发生)
        var queryTime = timeV0.AddMinutes(1).ToString("yyyy-MM-ddTHH:mm:ssZ");
        
        using var lfTime = LazyFrame.ScanDelta(rootUrl, datetime: queryTime, cloudOptions: options);
        var dfTime = lfTime.Collect();

        Assert.Equal(1, dfTime.Height); // 应该还是 V0
        Assert.Equal("V0", dfTime["Name"].ToArray<string>()[0]);
        Console.WriteLine($"Test C (Time Travel to {queryTime}) Passed");
    }
    [Fact]
    [Trait("DeltaLake", "FullCycle")]
    public void Test_Sink_And_Scan_Delta_Full_Cycle_Modes()
    {
        // ==========================================
        // 1. 环境与鉴权准备
        // ==========================================
        var tableName = $"delta_modes_{Guid.NewGuid()}";
        var rootUrl = $"s3://{_minio.BucketName}/{tableName}";
        
        var rawEndpoint = _minio.Endpoint.Replace("http://", "").Replace("https://", "").TrimEnd('/');
        var polarsEndpoint = $"http://{rawEndpoint}";
        
        var options = CloudOptions.Aws(
            region: _minio.Region,
            accessKey: _minio.AccessKey,
            secretKey: _minio.SecretKey,
            endpoint: polarsEndpoint
        );

        // 核心配置：MinIO 兼容性 + 原子提交支持
        options.Credentials!["AWS_ALLOW_HTTP"] = "true";
        options.Credentials!["aws_allow_http"] = "true";
        options.Credentials!["AWS_S3_FORCE_PATH_STYLE"] = "true";
        options.Credentials!["aws_s3_force_path_style"] = "true";
        options.Credentials!["AWS_S3_ALLOW_UNSAFE_RENAME"] = "true";
        
        // ==========================================
        // 2. [Mode: Append] 初始化写入 (Version 0 + 1)
        // ==========================================
        Console.WriteLine("Step 1: Initial Write (Append)...");
        using (var df = DataFrame.FromColumns(new { 
            Id = new[] { 1, 2 }, 
            Msg = new[] { "V1_A", "V1_B" } 
        }))
        {
            // 此时表不存在，Append 会触发 Create Table
            df.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // 验证 V1
        using var dfV1 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(2, dfV1.Height);

        // ==========================================
        // 3. [Mode: Append] 追加写入 (Version 2)
        // ==========================================
        Console.WriteLine("Step 2: Appending Data (Append)...");
        using (var df2 = DataFrame.FromColumns(new { 
            Id = new[] { 3 }, 
            Msg = new[] { "V2_C" } 
        }))
        {
            df2.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Append, cloudOptions: options);
        }

        // 验证 V2 (总共 3 行)
        using var dfV2 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(3, dfV2.Height);
        Assert.Contains("V1_A", dfV2["Msg"].ToArray<string>());
        Assert.Contains("V2_C", dfV2["Msg"].ToArray<string>());

        // ==========================================
        // 4. [Mode: Overwrite] 覆盖写入 (Version 3)
        // ==========================================
        // 预期：之前的 3 行数据逻辑删除，只保留新的 1 行数据
        Console.WriteLine("Step 3: Overwriting Data (Overwrite)...");
        using (var dfOverwrite = DataFrame.FromColumns(new { 
            Id = new[] { 999 }, 
            Msg = new[] { "Overwrite_New" } 
        }))
        {
            dfOverwrite.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Overwrite, cloudOptions: options);
        }

        // 验证 V3 (应该只有 1 行)
        using var dfV3 = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(1, dfV3.Height);
        Assert.Equal("Overwrite_New", dfV3["Msg"].ToArray<string>()[0]);
        
        // 验证 V3 的历史 (Time Travel Check)
        // 我们可以回溯到 V2 确认旧数据还在历史里
        using var dfBackToV2 = LazyFrame.ScanDelta(rootUrl, version: 2, cloudOptions: options).Collect();
        Assert.Equal(3, dfBackToV2.Height);

        // ==========================================
        // 5. [Mode: ErrorIfExists] 冲突检测
        // ==========================================
        // 预期：表已存在，应抛出异常
        Console.WriteLine("Step 4: Testing ErrorIfExists...");
        using (var dfError = DataFrame.FromColumns(new { Id = new[] { 0 } }))
        {
            var ex = Assert.Throws<PolarsException>(() => 
            {
                dfError.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.ErrorIfExists, cloudOptions: options);
            });
            
            // 验证 Rust 返回的错误信息
            Assert.Contains("Table already exists", ex.Message);
        }

        // ==========================================
        // 6. [Mode: Ignore] 忽略写入
        // ==========================================
        // 预期：表已存在，不报错，但不写入任何数据 (Height 保持为 1)
        Console.WriteLine("Step 5: Testing Ignore...");
        using (var dfIgnore = DataFrame.FromColumns(new { 
            Id = new[] { 888 }, 
            Msg = new[] { "Should_Not_Exist" } 
        }))
        {
            dfIgnore.Lazy().SinkDelta(rootUrl, mode: DeltaSaveMode.Ignore, cloudOptions: options);
        }

        // 验证数据未变 (依然是 Overwrite 后的那 1 行)
        using var dfFinal = LazyFrame.ScanDelta(rootUrl, cloudOptions: options).Collect();
        Assert.Equal(1, dfFinal.Height);
        Assert.DoesNotContain("Should_Not_Exist", dfFinal["Msg"].ToArray<string>());

        Console.WriteLine("Delta Streaming Full Cycle (Append/Overwrite/Error/Ignore) Passed!");
    }
}
