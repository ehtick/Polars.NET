using Amazon.S3;
using Testcontainers.Minio;

namespace Polars.Integration.Tests.Fixtures;

public class MinioFixture : IAsyncLifetime
{
    private readonly MinioContainer _minioContainer;

    // 固定凭证，方便调试
    public string AccessKey => "admin";
    public string SecretKey => "password";
    public string BucketName => "polars-test";
    public string Region => "us-east-1";

    public MinioFixture()
    {
        _minioContainer = new MinioBuilder("minio/minio:latest")
            .WithUsername(AccessKey)
            .WithPassword(SecretKey)
            .Build();
    }

    // 获取 Polars 需要的 Endpoint (例如 http://127.0.0.1:54321)
    public string Endpoint => _minioContainer.GetConnectionString();

    public async Task InitializeAsync()
    {
        // 1. 启动容器
        await _minioContainer.StartAsync();

        // 2. 初始化 Bucket (Polars Sink 不会自动创建 Bucket)
        var s3Config = new AmazonS3Config
        {
            ServiceURL = Endpoint,
            ForcePathStyle = true, // MinIO 必须开启这个
            UseHttp = true
        };

        using var s3Client = new AmazonS3Client(AccessKey, SecretKey, s3Config);

        try 
        {
            await s3Client.PutBucketAsync(BucketName);
        }
        catch (Exception ex)
        {
            // 如果 bucket 已存在或者其他问题，打印出来方便调试
            Console.WriteLine($"Error creating bucket: {ex.Message}");
            throw;
        }
    }

    public Task DisposeAsync()
    {
        return _minioContainer.DisposeAsync().AsTask();
    }
}