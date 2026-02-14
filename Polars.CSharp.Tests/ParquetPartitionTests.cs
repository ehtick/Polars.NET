namespace Polars.CSharp.Tests;

public class ParquetPartitionTests : IDisposable
{
    private readonly string _testBaseDir;

    public ParquetPartitionTests()
    {
        // 为每个测试创建一个唯一的临时目录
        _testBaseDir = Path.Combine(Path.GetTempPath(), "polars_net_partition_test_" + Guid.NewGuid());
        Directory.CreateDirectory(_testBaseDir);
    }

    public void Dispose()
    {
        // 清理测试产生的文件
        if (Directory.Exists(_testBaseDir))
        {
            try { Directory.Delete(_testBaseDir, true); } catch { /* Ignore cleanup errors */ }
        }
    }

    [Fact]
    public void SinkParquetPartitioned_ScanParquet_EndToEnd()
    {
        // ---------------------------------------------------
        // 1. 准备数据 (Prepare Data)
        // ---------------------------------------------------
        // 创建一个包含 'Group' (分区键) 和 'Value' (数据) 的 DataFrame
        var sGroup = new Series("Group", ["A", "A", "B", "B", "C"]);
        var sValue = new Series("Value", [1, 2, 3, 4, 5]);
        var df = new DataFrame(sGroup, sValue);

        // ---------------------------------------------------
        // 2. 分区写入 (Sink Parquet Partitioned)
        // ---------------------------------------------------
        // 我们按照 "Group" 列进行分区
        var lf = df.Lazy();
        
        // 注意：Selector.Col("Group") 需要你的 Selector 类支持这种工厂方法
        // 如果不支持，请使用 new Selector("Group") 或对应构造函数
        lf.SinkParquetPartitioned(
            _testBaseDir,
            partitionBy: Selector.Cols("Group"), 
            includeKeys: true // 在 Parquet 文件内部也保留该列（可选，Hive 模式通常通过文件夹名推断）
        );

        // ---------------------------------------------------
        // 3. 物理结构验证 (Physical Verification)
        // ---------------------------------------------------
        // 验证是否生成了 Hive 风格的目录结构: Group=A, Group=B, Group=C
        Assert.True(Directory.Exists(Path.Combine(_testBaseDir, "Group=A")), "Partition directory Group=A missing");
        Assert.True(Directory.Exists(Path.Combine(_testBaseDir, "Group=B")), "Partition directory Group=B missing");
        Assert.True(Directory.Exists(Path.Combine(_testBaseDir, "Group=C")), "Partition directory Group=C missing");

        // ---------------------------------------------------
        // 4. 分区扫描 (Scan Parquet)
        // ---------------------------------------------------
        // ScanParquet 指向根目录，Polars 会自动发现 Hive 分区
        var lfScan = LazyFrame.ScanParquet(
            _testBaseDir,
            glob: true,             // 开启 glob 模式以递归查找
            tryParseHiveDates: true // 尝试解析 Hive 分区中的日期（如果有）
        );

        var dfResult = lfScan.Collect();

        // ---------------------------------------------------
        // 5. 数据一致性验证 (Assertion)
        // ---------------------------------------------------
        // 由于分区写入和读取不保证行的顺序，我们需要排序后比较
        // 按照 Value 排序，因为 Value 在这个测试集中是唯一的
        
        // 对原始数据排序
        var dfExpected = df.Sort("Value");
        
        // 对结果数据排序
        var dfActual = dfResult.Sort("Value");

        // 验证行数
        Assert.Equal(dfExpected.Height, dfActual.Height);

        // 验证列内容 (Value)
        var expectedValues = dfExpected["Value"].ToArray<int>();
        var actualValues = dfActual["Value"].ToArray<int>();
        Assert.Equal(expectedValues, actualValues);

        // 验证分区列 (Group) 是否正确被读取回来
        var expectedGroups = dfExpected["Group"].ToArray<string>();
        var actualGroups = dfActual["Group"].ToArray<string>();
        Assert.Equal(expectedGroups, actualGroups);
    }
}
