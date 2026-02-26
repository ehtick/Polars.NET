// using Polars.CSharp;
// using Polars.Integration.Tests.Fixtures;

// namespace Polars.Integration.Tests;

// public class GcpTests : IClassFixture<GcpFixture>
// {
//     private readonly GcpFixture _gcp;

//     public GcpTests(GcpFixture gcp)
//     {
//         _gcp = gcp;
//     }

//     [Fact]
//     public void Test_RoundTrip_Parquet_Gcp_FakeServer()
//     {
//         var gcsUrl = $"gs://{GcpFixture.BucketName}/test_gcp.parquet";

//         // =========================================================
//         // 终极绝招：使用进程级环境变量劫持 Rust 底层配置
//         // =========================================================
//         // object_store 库会优先读取这些环境变量，这比 CloudOptions 传参更底层、更暴力。
//         Environment.SetEnvironmentVariable("GOOGLE_SERVICE_ACCOUNT", _gcp.FakeServiceAccountJsonPath);
//         Environment.SetEnvironmentVariable("GOOGLE_STORAGE_BASE_URL", _gcp.Endpoint);
//         Environment.SetEnvironmentVariable("GOOGLE_STORAGE_ALLOW_HTTP", "true");
//         Environment.SetEnvironmentVariable("GOOGLE_DISABLE_OAUTH", "true");

//         try
//         {
//             // 这里我们甚至不需要传 CloudOptions 了，或者传个空的 Gcp Provider
//             // 因为 Rust 会从环境变量里捡配置
//             // 但为了保险，我们传一个最基础的 Gcp Provider 标识
//             var options = new CloudOptions { Provider = CloudProvider.Gcp };

//             using var df = DataFrame.FromColumns(new
//             {
//                 Product = new[] { "Pixel", "Nexus", "Android" },
//                 Version = new[] { 8, 5, 14 }
//             });
            
//             // Write
//             df.Lazy().SinkParquet(gcsUrl, cloudOptions: options);

//             // Read
//             using var lfRead = LazyFrame.ScanParquet(gcsUrl, cloudOptions: options);
//             using var dfRead = lfRead.Collect();

//             // Verify
//             Assert.Equal(df.Height, dfRead.Height);
//             Assert.Equal("Pixel", dfRead["Product"][0]);
            
//             Console.WriteLine("GCP (Fake-GCS-Server) Round-Trip Test Passed!");
//         }
//         finally
//         {
//             // 清理环境变量，防止污染其他测试（虽然目前只有这一个涉及 GCP）
//             Environment.SetEnvironmentVariable("GOOGLE_SERVICE_ACCOUNT", null);
//             Environment.SetEnvironmentVariable("GOOGLE_STORAGE_BASE_URL", null);
//             Environment.SetEnvironmentVariable("GOOGLE_STORAGE_ALLOW_HTTP", null);
//             Environment.SetEnvironmentVariable("GOOGLE_DISABLE_OAUTH", null);
//         }
//     }
// }