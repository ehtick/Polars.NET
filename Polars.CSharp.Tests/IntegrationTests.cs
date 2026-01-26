using Microsoft.Data.SqlClient;
using Polars.CSharp.Tests.Fixtures;

namespace Polars.CSharp.Tests
{
    // 注入我们刚才写的 Fixture
    public class IntegrationTests : IClassFixture<MsSqlFixture>
    {
        private readonly MsSqlFixture _fixture;

        public IntegrationTests(MsSqlFixture fixture)
        {
            _fixture = fixture;
        }

        [Fact]  
        public async Task Test_RealSqlServer_ETL_EndToEnd_WithNulls()
        {
            // 1. 准备数据库环境 (DDL)
            // Region, Amount, OrderDate 默认都是允许 NULL 的
            var tableName = "Orders_" + Guid.NewGuid().ToString("N");
            var setupSql = $@"
                CREATE TABLE {tableName} (
                    OrderId INT PRIMARY KEY,
                    Region NVARCHAR(50) NULL,
                    Amount FLOAT NULL,
                    OrderDate DATETIME2 NULL
                );";

            using (var conn = new SqlConnection(_fixture.ConnectionString))
            {
                await conn.OpenAsync();
                using var cmd = new SqlCommand(setupSql, conn);
                await cmd.ExecuteNonQueryAsync();
            }

            // 2. 准备 Polars 数据 (Source) - 构造包含 Null 的数据
            int totalRows = 10000;
            var baseTime = DateTime.UtcNow.Date;

            // 构造数据生成逻辑：
            // - Region: 每 100 行插入一个 null
            // - Amount: 每 50 行插入一个 null
            // - OrderDate: 每 10 行插入一个 null (高频 null 测试)
            var df = DataFrame.FromColumns(new
            {
                OrderId = Enumerable.Range(0, totalRows).ToArray(),
                
                // 注意：这里必须显式使用 string?[] 等可空数组类型
                Region = Enumerable.Range(0, totalRows)
                    .Select(i => i % 100 == 0 ? null : "US")
                    .ToArray(),
                
                Amount = Enumerable.Range(0, totalRows)
                    .Select(i => i % 50 == 0 ? (double?)null : 100.5)
                    .ToArray(),

                OrderDate = Enumerable.Range(0, totalRows)
                    .Select(i => i % 10 == 0 ? (DateTime?)null : baseTime)
                    .ToArray()
            });

            // 3. 执行 ETL (SinkTo)
            await Task.Run(() =>
            {
                // 定义契约：即使是可空类型，Type 这里通常还是写基础类型
                // DataReader 会根据值是否为 null 自动处理 DBNull.Value
                var overrides = new Dictionary<string, Type>
                {
                    { "OrderDate", typeof(DateTime) } 
                };

                df.Lazy().SinkTo(reader =>
                {
                    using var bulk = new SqlBulkCopy(_fixture.ConnectionString);
                    bulk.DestinationTableName = tableName;
                    
                    // 开启流式写入配置（可选，提升大字段性能）
                    bulk.EnableStreaming = true; 
                    bulk.BatchSize = 2000;

                    bulk.ColumnMappings.Add("OrderId", "OrderId");
                    bulk.ColumnMappings.Add("Region", "Region");
                    bulk.ColumnMappings.Add("Amount", "Amount");
                    bulk.ColumnMappings.Add("OrderDate", "OrderDate");

                    try
                    {
                        bulk.WriteToServer(reader);
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"Bulk Copy Failed: {ex.Message}", ex);
                    }

                }, bufferSize: 100, typeOverrides: overrides);
            });

            // 4. 验证 (Verify)
            using (var conn = new SqlConnection(_fixture.ConnectionString))
            {
                await conn.OpenAsync();

                // 4.1 验证总行数
                using var cmdCount = new SqlCommand($"SELECT COUNT(*) FROM {tableName}", conn);
                Assert.Equal(totalRows, (int)await cmdCount.ExecuteScalarAsync());

                // 4.2 验证 Null 写入情况
                // 检查 OrderId = 0 (它是 100, 50, 10 的公倍数，所以三个字段都应该是 NULL)
                using var cmdNullCheck = new SqlCommand(
                    $"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 0", conn);
                
                using var reader = await cmdNullCheck.ExecuteReaderAsync();
                Assert.True(await reader.ReadAsync());

                // 断言数据库里真的是 DBNull
                Assert.True(await reader.IsDBNullAsync(0), "Region should be NULL for ID 0"); // Region
                Assert.True(await reader.IsDBNullAsync(1), "Amount should be NULL for ID 0"); // Amount
                Assert.True(await reader.IsDBNullAsync(2), "OrderDate should be NULL for ID 0"); // OrderDate
                reader.Close();

                // 4.3 验证非 Null 写入情况
                // 检查 OrderId = 1 (应该都有值)
                using var cmdValueCheck = new SqlCommand(
                    $"SELECT Region, Amount, OrderDate FROM {tableName} WHERE OrderId = 1", conn);
                
                var valResult = await cmdValueCheck.ExecuteReaderAsync();
                Assert.True(await valResult.ReadAsync());

                Assert.Equal("US", valResult["Region"]);
                Assert.Equal(100.5, Convert.ToDouble(valResult["Amount"])); // float 可能会有微小精度差异，这里简单对比
                Assert.Equal(baseTime, valResult["OrderDate"]);
            }
        }
    }
}