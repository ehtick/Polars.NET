using Polars.CSharp;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace Polars.Integration.Tests.Fixtures;

public class HttpFixture : IAsyncLifetime
{
    private readonly IContainer _nginxContainer;
    private string _localParquetPath = null!;

    public const int ContainerPort = 80;

    public HttpFixture()
    {
        // 使用最轻量的 Nginx
        _nginxContainer = new ContainerBuilder("nginx:alpine")
            .WithPortBinding(ContainerPort, true)
            .Build();
    }

    public string BaseUrl => $"http://{_nginxContainer.Hostname}:{_nginxContainer.GetMappedPublicPort(ContainerPort)}";

    public async Task InitializeAsync()
    {
        // 1. 先启动容器
        await _nginxContainer.StartAsync();

        // 2. 在本地生成一个 Parquet 文件
        _localParquetPath = Path.GetTempFileName();
        
        // 创建一个简单的 DataFrame 并保存为 Parquet
        using var df = DataFrame.FromColumns(new
        {
            Name = new[] { "HTTP", "Test" },
            Value = new[] { 123, 456 }
        });
        df.WriteParquet(_localParquetPath);

        // 3. 将这个文件复制到 Nginx 容器的默认目录
        // Testcontainers 支持把本地文件 copy 进容器
        var fileContent = await File.ReadAllBytesAsync(_localParquetPath);
        
        // CopyAsync 或者是 ExecAsync 写文件
        // Nginx 默认目录是 /usr/share/nginx/html
        await _nginxContainer.ExecAsync(new[] 
        { 
            "sh", "-c", $"echo 'Hello' > /usr/share/nginx/html/index.html" // 随便测试一下
        });
        
        // Testcontainers 的 CopyFileAsync API 
        await _nginxContainer.CopyAsync(fileContent, "/usr/share/nginx/html/data.parquet");
    }

    public Task DisposeAsync()
    {
        if (File.Exists(_localParquetPath)) File.Delete(_localParquetPath);
        return _nginxContainer.DisposeAsync().AsTask();
    }
}