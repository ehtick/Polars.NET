using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;

namespace Polars.Integration.Tests;

public class AzureTests : IClassFixture<AzuriteFixture>
{
    private readonly AzuriteFixture _azurite;

    public AzureTests(AzuriteFixture azurite)
    {
        _azurite = azurite;
    }

    [Fact]
    public void Test_RoundTrip_Parquet_Azure_Azurite()
    {
        // 1. 构造 Azure 路径 (使用 az:// scheme)
        // 格式: az://{container}/{blob}
        var azureUrl = $"az://{_azurite.ContainerName}/test_azure.parquet";

        // 2. 配置 CloudOptions
        var options = CloudOptions.Azure(
            accountName: AzuriteFixture.AccountName,
            accessKey: AzuriteFixture.AccountKey,
            endpoint: _azurite.BlobEndpoint // 必须传这个，因为是本地端口
        );

        // 3. 关键配置：告诉底层这是模拟器/HTTP
        // ObjectStore 对 Azure 模拟器的支持通常需要这个 flag
        // 或者 azure_allow_http = "true"
        if (options.Credentials != null)
        {
            options.Credentials["azure_allow_http"] = "true";
            // 有些版本的 object_store 可能需要这个来关闭 SSL 验证
            options.Credentials["azure_use_emulator"] = "true"; 
        }

        // 4. 准备数据
        using var df = DataFrame.FromColumns(new
        {
            City = new[] { "Seattle", "Redmond", "Bellevue" },
            Temp = new[] { 15.5, 16.0, 15.2 }
        });

        // 5. Round-Trip 测试
        
        // Write (Sink)
        df.Lazy().SinkParquet(azureUrl, cloudOptions: options);

        // Read (Scan)
        using var lfRead = LazyFrame.ScanParquet(azureUrl, cloudOptions: options);
        using var dfRead = lfRead.Collect();

        // 6. 验证
        Assert.Equal(df.Height, dfRead.Height);
        Assert.Equal("Seattle", dfRead["City"][0]);
        
        Console.WriteLine("Azure (Azurite) Round-Trip Test Passed!");
    }
}