using Polars.CSharp;
using Polars.Integration.Tests.Fixtures;

namespace Polars.Integration.Tests;

public class HttpTests : IClassFixture<HttpFixture>
{
    private readonly HttpFixture _http;

    public HttpTests(HttpFixture http)
    {
        _http = http;
    }

    [Fact]
    public void Test_ScanParquet_From_Http_Nginx()
    {
        // 1. 构造 HTTP URL
        var httpUrl = $"{_http.BaseUrl}/data.parquet";
        Console.WriteLine($"Testing HTTP URL: {httpUrl}");

        // 2. 配置 CloudOptions (虽然是 HTTP，通常不需要凭证，但可以测试一下 Header 机制)
        // 这里演示如果需要 Token 怎么传
        var options = CloudOptions.Http(new Dictionary<string, string>
        {
            // Nginx 默认不需要 Auth，但传了也不会报错，刚好测试 Polars 接收 Header 的逻辑
            { "Authorization", "Bearer fake-token" },
            { "User-Agent", "Polars.NET-Test-Client" }
        });

        // 3. Scan (Read)
        // 注意：HTTP 源通常不支持 glob 操作，也不支持写
        using var lf = LazyFrame.ScanParquet(httpUrl, cloudOptions: options);
        using var df = lf.Collect();

        // 4. Verify
        Assert.Equal(2, df.Height);
        Assert.Equal("HTTP", df["Name"][0]);
        Assert.Equal(123, df["Value"][0]);
        
        Console.WriteLine("HTTP (Nginx) Scan Test Passed!");
    }
}