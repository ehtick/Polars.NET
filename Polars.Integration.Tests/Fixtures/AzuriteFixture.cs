using Azure.Storage.Blobs;
using Testcontainers.Azurite;

namespace Polars.Integration.Tests.Fixtures;

public class AzuriteFixture : IAsyncLifetime
{
    private readonly AzuriteContainer _azuriteContainer;

    // Azurite 默认固定凭证 (这是公开的 secrets，不用担心泄漏)
    // 账号: devstoreaccount1
    public const string AccountName = "devstoreaccount1";
    // Key: 
    public const string AccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    
    public string ContainerName => "polars-azure-test";

    public AzuriteFixture()
    {
        // 使用官方 Azurite 镜像
        _azuriteContainer = new AzuriteBuilder("mcr.microsoft.com/azure-storage/azurite:latest")
            .Build();
    }

    public string ConnectionString => _azuriteContainer.GetConnectionString();
    
    // 获取 HTTP 端点 (Polars 需要这个)
    // Azurite 默认 Blob 端口是 10000
    // Testcontainers 会映射到随机端口，我们从 ConnectionString 里解析或者直接用 GetConnectionString
    // 但 Polars 需要的是 "endpoint url"，通常是 http://127.0.0.1:xxxxx/devstoreaccount1
    public string BlobEndpoint 
    {
        get
        {
            // 解析 ConnectionString 稍微有点麻烦，但这是最稳的方法
            // 格式通常是: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=...;BlobEndpoint=http://127.0.0.1:49321/devstoreaccount1;"
            var parts = ConnectionString.Split(';');
            foreach (var part in parts)
            {
                if (part.StartsWith("BlobEndpoint="))
                {
                    return part.Substring("BlobEndpoint=".Length);
                }
            }
            throw new Exception("Could not find BlobEndpoint in connection string");
        }
    }

    public async Task InitializeAsync()
    {
        await _azuriteContainer.StartAsync();

        // 使用官方 SDK 创建 Container (Bucket)
        var options = new BlobClientOptions(BlobClientOptions.ServiceVersion.V2025_11_05);
        
        // 将 options 传给 Client
        var blobServiceClient = new BlobServiceClient(ConnectionString, options);
        var containerClient = blobServiceClient.GetBlobContainerClient(ContainerName);
        await containerClient.CreateIfNotExistsAsync();
    }

    public Task DisposeAsync()
    {
        return _azuriteContainer.DisposeAsync().AsTask();
    }
}