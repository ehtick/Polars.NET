// using DotNet.Testcontainers.Builders;
// using DotNet.Testcontainers.Containers;
// using Google.Apis.Auth.OAuth2;
// using Google.Cloud.Storage.V1;
// using System.Security.Cryptography;

// namespace Polars.Integration.Tests.Fixtures;

// public class GcpFixture : IAsyncLifetime
// {
//     private readonly IContainer _gcpContainer;

//     // 伪造的 Project ID
//     public const string ProjectId = "test-project";
//     public const string BucketName = "polars-gcp-test";

//     public GcpFixture()
//     {
//         // 使用 fsouza/fake-gcs-server
//         // -scheme http: 强制使用 HTTP
//         _gcpContainer = new ContainerBuilder()
//             .WithImage("fsouza/fake-gcs-server:latest")
//             .WithPortBinding(4443, true) // 容器内部默认端口
//             .WithCommand("-scheme", "http") 
//             .Build();
//     }

//     // 获取 Endpoint (例如 http://127.0.0.1:32768)
//     public string Endpoint => $"http://{_gcpContainer.Hostname}:{_gcpContainer.GetMappedPublicPort(4443)}";

//     // 伪造一个 Service Account JSON 路径
//     // Polars 需要这个文件存在，且格式正确，才能初始化 GCS 客户端
//     public string FakeServiceAccountJsonPath { get; private set; } = null!;

//     public async Task InitializeAsync()
//     {
//         await _gcpContainer.StartAsync();

//         // 1. 动态生成一个合法的 RSA 私钥 (PKCS#8 格式)
//         // 这样 Google SDK 绝对挑不出毛病
//         using var rsa = RSA.Create(2048);
        
//         // ExportPkcs8PrivateKeyPem 是 .NET Core 3.0+ 原生支持的方法
//         // 它会自动带上 -----BEGIN PRIVATE KEY----- 头和换行符
//         var privateKeyPem = rsa.ExportPkcs8PrivateKeyPem();

//         // JSON 里的 private_key 字段需要把换行符转义成 \n
//         var escapedPrivateKey = privateKeyPem.Replace("\r\n", "\\n").Replace("\n", "\\n");

//         var jsonContent = $@"
// {{
//   ""type"": ""service_account"",
//   ""project_id"": ""{ProjectId}"",
//   ""private_key_id"": ""fake-key-id"",
//   ""private_key"": ""{escapedPrivateKey}"",
//   ""client_email"": ""test@{ProjectId}.iam.gserviceaccount.com"",
//   ""client_id"": ""123456789"",
//   ""auth_uri"": ""https://accounts.google.com/o/oauth2/auth"",
//   ""token_uri"": ""https://oauth2.googleapis.com/token"",
//   ""auth_provider_x509_cert_url"": ""https://www.googleapis.com/oauth2/v1/certs"",
//   ""client_x509_cert_url"": ""https://www.googleapis.com/robot/v1/metadata/x509/test%40{ProjectId}.iam.gserviceaccount.com""
// }}";

//         FakeServiceAccountJsonPath = Path.GetTempFileName();
//         await File.WriteAllTextAsync(FakeServiceAccountJsonPath, jsonContent);

//         var clientBuilder = new StorageClientBuilder
//         {
//             UnauthenticatedAccess = true,
//             BaseUri = Endpoint,
//             GoogleCredential = GoogleCredential.FromJson(jsonContent)
//         };
        
//         var client = await clientBuilder.BuildAsync();

//         for (int i = 0; i < 5; i++)
//         {
//             try
//             {
//                 await client.CreateBucketAsync(ProjectId, BucketName);
//                 break;
//             }
//             catch
//             {
//                 await Task.Delay(500);
//             }
//         }
//     }

//     public async Task DisposeAsync()
//     {
//         if (File.Exists(FakeServiceAccountJsonPath))
//         {
//             File.Delete(FakeServiceAccountJsonPath);
//         }
//         await _gcpContainer.DisposeAsync();
//     }
// }