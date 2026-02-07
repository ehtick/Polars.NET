using System.Threading.Tasks;
using Testcontainers.MsSql;
using Xunit;
using Microsoft.Data.SqlClient; // 需要安装 System.Data.SqlClient 或 Microsoft.Data.SqlClient 包

namespace Polars.Integration.Tests.Fixtures
{
    // 实现 IAsyncLifetime 接口，xUnit 会自动处理 StartAsync (测试前) 和 DisposeAsync (测试后)
    public class MsSqlFixture : IAsyncLifetime
    {
        private readonly MsSqlContainer _msSqlContainer;

        public MsSqlFixture()
        {
            // 构建一个 SQL Server 2022 容器
            _msSqlContainer = new MsSqlBuilder("mcr.microsoft.com/mssql/server:2022-latest")
            .Build();
        }

        public string ConnectionString => _msSqlContainer.GetConnectionString();

        public async Task InitializeAsync()
        {
            // 1. 启动容器
            await _msSqlContainer.StartAsync();

            // 2. 初始化数据 (Seeding)
            // 既然是集成测试，我们通常需要预先建表塞数据，方便后续 polars.read_sql 测试
            await InitializeDatabaseDataAsync();
        }

        public Task DisposeAsync()
        {
            // xUnit 结束时会自动销毁容器，无需手动 docker rm
            return _msSqlContainer.DisposeAsync().AsTask();
        }

        private async Task InitializeDatabaseDataAsync()
        {
            using var connection = new SqlConnection(ConnectionString);
            await connection.OpenAsync();

            var cmdText = @"
                CREATE TABLE TestData (
                    Id INT PRIMARY KEY,
                    Name NVARCHAR(50),
                    Value FLOAT,
                    IsActive BIT
                );

                INSERT INTO TestData (Id, Name, Value, IsActive) VALUES 
                (1, 'Alice', 12.5, 1),
                (2, 'Bob', 33.3, 0),
                (3, 'Charlie', 0.0, 1);
            ";

            using var command = new SqlCommand(cmdText, connection);
            await command.ExecuteNonQueryAsync();
        }
    }
}