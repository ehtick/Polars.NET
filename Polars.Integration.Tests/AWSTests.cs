using Polars.Integration.Tests.Fixtures;
using Polars.CSharp;

namespace Polars.Integration.Tests;

public class AwsTests : IClassFixture<MinioFixture>
{
    private readonly MinioFixture _minio;

    public AwsTests(MinioFixture minio)
    {
        _minio = minio;
    }

    [Fact]
    public void Test_RoundTrip_Parquet_AWS()
    {
        // ==========================================
        // 1. 准备环境 & 数据
        // ==========================================
        var s3Url = $"s3://{_minio.BucketName}/test_roundtrip.parquet";
        
        // 构造 CloudOptions
        // 注意：MinIO 是 HTTP，Polars 默认强制 HTTPS，必须加 aws_allow_http
        var options = CloudOptions.Aws(
            region: _minio.Region,
            accessKey: _minio.AccessKey,
            secretKey: _minio.SecretKey,
            endpoint: _minio.Endpoint
        );
        options.Credentials!["aws_allow_http"] = "true";
        // 强制 Path Style (MinIO 需要，否则它会尝试访问 bucket.localhost)
        options.Credentials!["aws_s3_force_path_style"] = "true"; 

        // 创建内存数据
        using var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1, 2, 3, 4, 5 },
            Name = new[] { "Alice", "Bob", "Charlie", "David", "Eve" },
            Score = new[] { 99.5, 88.0, 75.5, 60.0, 100.0 }
        });

        // ==========================================
        // 2. Sink (写入) -> S3
        // ==========================================
        // Lazy() -> SinkParquet
        df.Lazy().SinkParquet(s3Url, cloudOptions: options);

        // ==========================================
        // 3. Scan (读取) <- S3
        // ==========================================
        using var lfRead = LazyFrame.ScanParquet(s3Url, cloudOptions: options);
        using var dfRead = lfRead.Collect();

        // ==========================================
        // 4. 验证
        // ==========================================
        Assert.Equal(df.Height, dfRead.Height);
        
        var originalNames = df["Name"].ToArray<string>();
        var readNames = dfRead["Name"].ToArray<string>();
        
        Assert.Equal(originalNames, readNames);
        
        Console.WriteLine("Cloud Round-Trip Test Passed!");
    }
}