using System.Text;
using System.Text.Json;
using Minio;
using Minio.DataModel.Args;

namespace Polars.Integration.Tests.Utils
{
    public static class DeltaLakeTestHelper
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        /// <summary>
        /// 生成 Delta Lake Schema 的 JSON 字符串
        /// </summary>
        public static string GenerateSchemaString()
        {
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
            return JsonSerializer.Serialize(schemaObj, JsonOptions);
        }

        /// <summary>
        /// 生成 "protocol" 动作 JSON
        /// </summary>
        public static string ActionProtocol(int minReader = 1, int minWriter = 2)
        {
            // {"protocol":{...}}
            var inner = new { minReaderVersion = minReader, minWriterVersion = minWriter };
            return $"{{\"protocol\":{JsonSerializer.Serialize(inner, JsonOptions)}}}";
        }

        /// <summary>
        /// 生成 "metaData" 动作 JSON (注意 D 大写)
        /// </summary>
        public static string ActionMetadata(string schemaString, long createdTimeFn = 0)
        {
            // {"metaData":{...}}
            var inner = new
            {
                id = Guid.NewGuid().ToString(),
                format = new { provider = "parquet", options = new { } },
                schemaString = schemaString, // 这里会自动再次转义，这是对的
                partitionColumns = new string[0],
                configuration = new { },
                createdTime = createdTimeFn == 0 ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : createdTimeFn
            };
            return $"{{\"metaData\":{JsonSerializer.Serialize(inner, JsonOptions)}}}";
        }

        /// <summary>
        /// 生成 "add" 动作 JSON
        /// </summary>
        public static string ActionAdd(string path, long size = 1000, long modTime = 0)
        {
            // {"add":{...}}
            var inner = new
            {
                path = path,
                partitionValues = new { },
                size = size,
                modificationTime = modTime == 0 ? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() : modTime,
                dataChange = true,
                stats = "{\"numRecords\":1}"
            };
            return $"{{\"add\":{JsonSerializer.Serialize(inner, JsonOptions)}}}";
        }

        /// <summary>
        /// 辅助方法：将生成的 Log 内容上传到 MinIO
        /// </summary>
        public static async Task UploadLogAsync(IMinioClient client, string bucket, string tableName, long version, string content)
        {
            // Delta Log 文件名格式: 00000000000000000000.json (20位)
            var fileName = version.ToString("D20") + ".json";
            var objectName = $"{tableName}/_delta_log/{fileName}";

            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(content));
            await client.PutObjectAsync(new PutObjectArgs()
                .WithBucket(bucket)
                .WithObject(objectName)
                .WithStreamData(stream)
                .WithObjectSize(stream.Length)
                .WithContentType("application/json"));
        }
    }
}