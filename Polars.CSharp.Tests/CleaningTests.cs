using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class CleaningTests
{
    [Fact]
    public void Test_Forward_Backward_Fill()
    {
        var content = "val\n1\n\n\n2\n\n"; 
        
        using var csv = new DisposableFile(content, ".csv");
        using var df = DataFrame.ReadCsv(csv.Path);
        
        // Forward Fill (limit=null -> 0 -> Infinite)
        using var ff = df.Select(Col("val").ForwardFill().Alias("ff"));
        
        // 验证：
        // 1 (原值)
        // 1 (填充)
        // 1 (填充)
        // 2 (原值)
        // 2 (填充)
        Assert.Equal(1, ff.GetValue<int>(0,"ff"));
        Assert.Equal(1, ff.GetValue<int>(1,"ff")); 
        Assert.Equal(1, ff.GetValue<int>(2,"ff")); 
        Assert.Equal(2, ff.GetValue<int>(3,"ff"));
        Assert.Equal(2, ff.GetValue<int>(4,"ff")); 
    }
    [Fact]
    public void Test_Sampling()
    {
        // 准备 100 行数据
        var rows = Enumerable.Range(0, 100).Select(i => new { Val = i });
        using var df = DataFrame.From(rows);
        Assert.Equal(100, df.Height);

        // Sample N=10
        using var sampleN = df.Sample(n: 10, seed: 42);
        Assert.Equal(10, sampleN.Height);

        // Sample Frac=0.1 (10%)
        using var sampleFrac = df.Sample(fraction: 0.1, seed: 42);
        // 大约 10 行，具体取决于算法，但在 100 行这种小数据量下，Fixed Fraction 通常准确
        Assert.Equal(10, sampleFrac.Height);
    }
    [Fact]
    public void Test_Data_Cleaning_Trio()
    {
        // [关键] 无缩进 CSV
        var content = "A,B,C\n1,x,10\n,y,20\n3,,30\n";
        
        using var csv = new DisposableFile(content, ".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // --- 1. FillNull ---
        using var filledDf = df.WithColumns(
            Col("A").FillNull(0), 
            Col("B").FillNull("unknown")
        );
        
        Assert.Equal(0, filledDf.GetValue<int>(1,"A")); // null -> 0
        Assert.Equal("unknown", filledDf.GetValue<string>(2,"B")); // null -> unknown

        // --- 2. DropNulls ---
        using var dfDirty = DataFrame.ReadCsv(csv.Path);
        using var droppedDf = dfDirty.DropNulls();
        
        // Row 0: 1, x, 10 (完整) -> 保留
        // Row 1: null, y, 20 -> 删
        // Row 2: 3, null, 30 -> 删
        Assert.Equal(1, droppedDf.Height); 
        Assert.Equal(1, droppedDf.GetValue<int>(0,"A"));
    }
    [Fact]
    public void Test_Cleaning_Dirty_Data()
    {
        // 1. 模拟脏数据 (字符串列，混有数字、垃圾字符、NaN文字)
        var df = DataFrame.FromColumns(new 
        {
            RawData = new object[] { 100, 200.5, "NotANumber", "NaN", null }
        });

        // 2. 清洗流程
        var cleanExpr = Col("RawData")
            // Step A: 强转为 Double，strict=false
            // "100" -> 100.0
            // "200.5" -> 200.5
            // "NotANumber" -> null (因为非严格转换)
            // "NaN" -> NaN (Polars能识别字符串 "NaN")
            // null -> null
            .Cast(DataType.Float64, strict: false)
            
            // Step B: 处理 NaN (针对那行 "NaN")
            .FillNan(0) 
            
            // Step C: 处理 Null (针对那行 "NotANumber" 和原本的 null)
            .FillNull(0);

        var result = df.Select(cleanExpr.Alias("Cleaned"));

        // 3. 验证
        var rows = result["Cleaned"].ToArray<double?>();
        
        Assert.Equal(100.0, rows[0]);
        Assert.Equal(200.5, rows[1]);
        Assert.Equal(0.0, rows[2]); // "NotANumber" -> null -> 0
        Assert.Equal(0.0, rows[3]); // "NaN" -> NaN -> 0
        Assert.Equal(0.0, rows[4]); // null -> 0
    }
    [Fact]
    public void Test_Series_Unique_And_Duplicated()
    {
        // 数据: [1, 2, 2, 3]
        // IsUnique -> [T, F, F, T] (1和3是唯一的)
        // IsDuplicated -> [F, T, T, F] (2是重复的)
        
        using var s = Series.From("nums", [1, 2, 2, 3]);

        // 1. 测试 Unique (去重)
        using var unique = s.UniqueStable();
        Assert.Equal(3, unique.Length);
        Assert.Equal(1, unique[0]);
        Assert.Equal(2, unique[1]);
        Assert.Equal(3, unique[2]);

        // 2. 测试 IsDuplicated (掩码)
        using var dupMask = s.IsDuplicated();
        Assert.Equal(DataTypeKind.Boolean, dupMask.DataType.Kind);
        // 验证: 1->F, 2->T, 2->T, 3->F
        Assert.False((bool)dupMask[0]);
        Assert.True((bool)dupMask[1]);
        Assert.True((bool)dupMask[2]);
        Assert.False((bool)dupMask[3]);

        // 3. 测试 IsUnique (反向掩码: 只出现一次的才为 True)
        using var uniqMask = s.IsUnique();
        Assert.True((bool)uniqMask[0]);  // 1 只出现一次
        Assert.False((bool)uniqMask[1]); // 2 出现了两次
        Assert.False((bool)uniqMask[2]);
        Assert.True((bool)uniqMask[3]);  // 3 只出现一次
    }

    [Fact]
    public void Test_Expr_Unique_Context()
    {
        using var df = DataFrame.FromColumns(new 
        {
            Group = new[] { "A", "A", "B", "B", "B" },
            Val = new[]   { 1,   1,   2,   3,   2 }
        });

        // 测试在 GroupBy 上下文中去重
        // A组: [1, 1] -> unique -> [1]
        // B组: [2, 3, 2] -> unique -> [2, 3]
        
        using var res = df.Lazy()
            .GroupBy(Col("Group"))
            .Agg(
                Col("Val").Unique().Alias("UniqueVals"),
                Col("Val").IsDuplicated().Sum().Alias("DupCount") // 统计重复的数量
            )
            .Sort("Group")
            .Collect();

        // A组: UniqueVals=[1], DupCount=2 (因为两个1都是重复的一部分? Polars IsDuplicated 语义是: 是否标记重复)
        // Polars is_duplicated: 如果元素重复出现，第一次出现标记为 false (除非 keep='none')? 
        // 不，默认 is_duplicated() 会把所有重复项（除了第一个）标记为 true，或者取决于策略。
        // 实际上 Polars is_duplicated() 默认把所有重复的都标记为 True。
        
        // 让我们验证一下
        var groupA_DupCount = (uint)res["DupCount"][0]; 
        // A: [1, 1]. IsDuplicated -> [True, True] (因为1不是唯一的). Sum = 2.
        Assert.Equal(2u, groupA_DupCount);

        var groupB_DupCount = (uint)res["DupCount"][1];
        // B: [2, 3, 2]. IsDuplicated -> [True, False, True]. Sum = 2.
        Assert.Equal(2u, groupB_DupCount);
    }
    [Fact]
    public void TestDropNulls()
    {
        // 1. 测试 Series.DropNulls()
        var s = Series.From("data", new int?[] { 1, null, 2, null, 3 });
        var cleanSeries = s.DropNulls();

        Assert.Equal(3, cleanSeries.Length);

        // 2. 测试 Expr.DropNulls() via DataFrame
        // 注意：在 Select 中使用 DropNulls 会改变列长，所以只选这一列以避免 "Length Mismatch" 错误
        var df = DataFrame.FromColumns(new
        {
            vals = new int?[] { 10, null, 20 }
        });

        var resultDf = df.Select(Polars.Col("vals").DropNulls());
        
        Assert.Equal(2, resultDf.Height);
        Assert.Equal(10, resultDf["vals"][0]);
        Assert.Equal(20, resultDf["vals"][1]);
    }

    [Fact]
    public void TestDropNans()
    {
        // NaN (Not a Number) 是浮点数特有的概念，不同于 Null
        
        // 1. 测试 Series.DropNans()
        // 你的实现是用 ApplyExpr 做到的，这个测试也能验证 ApplyExpr 是否正确处理了长度变化
        var s = new Series("floats", new double[] { 1.5, double.NaN, 2.5, double.NaN, 3.5 });
        var cleanSeries = s.DropNans();

        Assert.Equal(3, cleanSeries.Length);
        
        // 验证内容 (CollectionAssert 比较浮点数可能需要 comparer，这里简单手写)
        var arr = cleanSeries.ToArray<double>();
        Assert.Equal(1.5, arr[0]);
        Assert.Equal(2.5, arr[1]);
        Assert.Equal(3.5, arr[2]);

        // 2. 测试 Expr.DropNans() via DataFrame
        var df = DataFrame.FromColumns(new
        {
            f = new double[] { double.NaN, 100.0, double.NaN }
        });

        var resultDf = df.Select(Polars.Col("f").DropNans());
        
        Assert.Equal(1, resultDf.Height);
        Assert.Equal(100.0, resultDf["f"][0]);
    }
}