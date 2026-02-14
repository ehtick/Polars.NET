using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class LazyFrameTests
{
    [Fact]
    public void Test_ScanCsv_Filter_Select()
    {
        // 1. 准备一个临时 CSV 文件
        var csvContent = @"name,age,salary
Alice,25,50000
Bob,30,60000
Charlie,35,70000
David,40,80000";
        using var csv = new DisposableFile(csvContent, ".csv");
        // 2. Scan CSV
        using var lf = LazyFrame.ScanCsv(csv.Path);
        using var lf_copyed = lf.Clone();
        using var df = lf.Collect();
        // 验证加载正确
        Assert.Equal(4, df.Height);
        Assert.Equal(3, df.Width);
        Assert.Contains("name", df.Columns);

        // 3. 执行操作：筛选 age > 30 并选择 name 和 salary
        // SQL 逻辑: SELECT name, salary FROM df WHERE age > 30
        using var filtered = lf_copyed
            .Filter(Col("age") > 30)
            .Select(Col("name"), Col("salary"));
        using var resultDf = filtered.Collect();
        // 验证结果
        // 应该剩下 Charlie (35) 和 David (40)
        Assert.Equal(2, resultDf.Height);
        Assert.Equal(2, resultDf.Width); // name, salary

        // 4. 验证具体值
        var nameCol = resultDf.Column("name");
        
        Assert.NotNull(nameCol);
        Assert.Equal("Charlie", nameCol.GetValue<string>(0));
        Assert.Equal("David", nameCol.GetValue<string>(1));

    }
    [Fact]
    public void Test_Lazy_Concat_Horizontal_And_Safety()
    {
        // 准备两个 LazyFrame
        using var csv1 = new DisposableFile("id\n1\n2",".csv");
        using var lf1 = LazyFrame.ScanCsv(csv1.Path);

        using var csv2 = new DisposableFile("name\nAlice\nBob",".csv");
        using var lf2 = LazyFrame.ScanCsv(csv2.Path);

        // 1. 执行 Horizontal Concat
        var concatLf = LazyFrame.Concat([lf1, lf2], ConcatType.Horizontal);
        
        // 收集结果
        using var df = concatLf.Collect();
        
        Assert.Equal(2, df.Height);
        Assert.Equal(2, df.Width); // id, name

        // 验证数据
        Assert.Equal(1, df.Column("id").GetValue<long>(0));
        Assert.Equal("Alice", df.Column("name").GetValue<string>(0));

        // 2. [关键] 验证 C# 对象安全性 (F# vs C# 习惯测试)
        
        // 我们复用 lf1 做另一个查询
        using var df1_again = lf1.Select(Col("id") * Lit(10)).Collect();
        Assert.Equal(2, df1_again.Height);
        
        Assert.Equal(10, df1_again.Column("id").GetValue<long>(0));
    }
    
    [Fact]
    public void Test_Lazy_Concat_Diagonal()
    {
        // LF1: [A, B]
        using var csv1 = new DisposableFile("A,B\n1,10",".csv");
        using var lf1 = LazyFrame.ScanCsv(csv1.Path);

        // LF2: [B, C]
        using var csv2 = new DisposableFile("B,C\n20,300",".csv");
        using var lf2 = LazyFrame.ScanCsv(csv2.Path);

        // Diagonal Concat (Lazy)
        // 结果结构预期：
        // Row 0 (来自 LF1): A=1,    B=10,   C=null (补位)
        // Row 1 (来自 LF2): A=null, B=20,   C=300
        var concatLf = LazyFrame.Concat([lf1, lf2], ConcatType.Diagonal);
        
        using var df = concatLf.Collect();
        
        Assert.Equal(2, df.Height);
        Assert.Equal(3, df.Width); // A, B, C

        // --- 验证 Row 0 (来自 LF1) ---
        // A 和 B 有值
        Assert.Equal(1, df.GetValue<int?>(0, "A"));
        Assert.Equal(10, df.GetValue<int?>(0, "B"));
        
        // C 应该是 null (因为 LF1 没有 C列)
        // [重点] 使用 Assert.Null + GetValue<int?>
        Assert.Null(df.GetValue<int?>(0, "C"));
        
        // --- 验证 Row 1 (来自 LF2) ---
        // A 应该是 null (因为 LF2 没有 A列)
        Assert.Null(df.GetValue<int?>(1, "A"));
        
        // B 和 C 有值
        Assert.Equal(20, df.GetValue<int?>(1, "B"));
        Assert.Equal(300, df.GetValue<int?>(1, "C"));
    }
    [Fact]
    public void Test_LazyFrame_Join_MultiColumn_WithParams()
    {
        // 1. 准备数据 (内存构建 -> Lazy)
        // 左表：Alice (2023, 2024), Bob (2023)
        // note 列用于测试 suffix
        using var scoresDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2024,    2023 },
            score   = new[] { 85,      90,      70 },
            note    = new[] { "L1",    "L2",    "L3" }
        });
        using var scoresLf = scoresDf.Lazy();

        // 右表：Alice (2023, 2024), Bob (2024)
        // note 列产生冲突
        using var classDf = DataFrame.FromColumns(new 
        {
            student = new[] { "Alice", "Alice", "Bob" },
            year    = new[] { 2023,    2024,    2024 },
            className = new[] { "Math", "Physics", "History" },
            note    = new[] { "R1",    "R2",    "R3" }
        });
        using var classLf = classDf.Lazy();

        // 2. 执行 Lazy Join (Inner)
        // 预期匹配：Alice 2023, Alice 2024
        using var joinedLf = scoresLf.Join(
            classLf,
            leftOn: [Col("student"), Col("year")],
            rightOn: [Col("student"), Col("year")],
            how: JoinType.Inner,
            
            // --- 新参数测试 ---
            suffix: "_lazy_conflict",            // 测试后缀
            validation: JoinValidation.OneToOne, // 测试 Lazy 模式下的验证 (Lazy 执行时会检查)
            coalesce: JoinCoalesce.JoinSpecific
        );

        // 3. Collect 执行
        using var joinedDf = joinedLf.Collect();

        // 4. 验证
        Assert.Equal(2, joinedDf.Height);
        Assert.Equal(6, joinedDf.Width); // student, year, score, note, className, note_lazy_conflict

        // 验证 Suffix 生效
        var cols = joinedDf.ColumnNames;
        Assert.Contains("note", cols);
        Assert.Contains("note_lazy_conflict", cols);

        // 验证数据准确性 (简单取第一行，Alice 2023)
        // 注意：Join 后的顺序不一定保证，除非 Sort，但在简单场景下通常稳定
        using var sorted = joinedDf.Sort("year");
        
        Assert.Equal(2023, sorted.GetValue<int>(0, "year"));
        Assert.Equal("Math", sorted.GetValue<string>(0, "className"));
        Assert.Equal("L1", sorted.GetValue<string>(0, "note"));
        Assert.Equal("R1", sorted.GetValue<string>(0, "note_lazy_conflict"));

        // 验证 Bob (2023) 被过滤 (Inner Join)
        using var bobCheck = joinedDf.Filter(Col("student") == Lit("Bob"));
        Assert.Equal(0, bobCheck.Height);
    }
    [Fact]
    public void Test_LazyFrame_GroupBy_Agg()
    {
         // 准备数据: Department, Salary
        var csvContent = @"dept,salary
IT,100
IT,200
HR,150
HR,50";
        using var scoresCsv = new DisposableFile(csvContent,".csv");


        using var lf = LazyFrame.ScanCsv(scoresCsv.Path);

        // GroupBy dept, Agg Sum(salary)
        using var groupedlf = lf
            .GroupBy("dept")
            .Agg(Col("salary").Sum().Alias("total_salary"))
            .Sort("total_salary", descending: true); // 排序方便断言
        var grouped = groupedlf.Collect();
        // 预期: 
        // IT: 300
        // HR: 200
        
        Assert.Equal(2, grouped.Height);
    
        Assert.Equal("IT", grouped.Column("dept").GetValue<string>(0));
        Assert.Equal(300, grouped.Column("total_salary").GetValue<long>(0));
        
        Assert.Equal("HR", grouped.Column("dept").GetValue<string>(1));
        Assert.Equal(200, grouped.Column("total_salary").GetValue<long>(1));
    }
    [Fact]
    public void Test_Lazy_Unpivot_With_Explain()
    {
        // 构造宽表数据: 日期, 苹果价格, 香蕉价格
        var content = @"date,apple,banana
2024-01-01,10,20
2024-01-02,12,22";
        
        using var csv = new DisposableFile(content,".csv");
        using var lf = LazyFrame.ScanCsv(csv.Path);

        // 构建 Lazy 查询: Unpivot (Melt)
        var unpivotedLf = lf.Unpivot(
            index: ["date"],
            on: ["apple", "banana"],
            variableName: "fruit",
            valueName: "price"
        );

        // --- Explain 功能测试 ---
        // 获取查询计划字符串
        string plan = unpivotedLf.Explain(optimized: true);
        Console.WriteLine("LazyFrame Explain Plan:");
        Console.WriteLine(plan);

        Assert.Contains("UNPIVOT", plan.ToUpper()); 

        // 执行查询
        using var df = unpivotedLf.Collect();

        // 验证结果
        // 原来 2 行，每行拆成 2 个水果 -> 总共 4 行
        Assert.Equal(4, df.Height);
        Assert.Equal(3, df.Width); // date, fruit, price
        
        // 简单验证第一行 (具体顺序取决于实现，但通常有序)
        // 检查列存在性
        Assert.NotNull(df.Column("fruit"));
        Assert.NotNull(df.Column("price"));
        
        // 验证值类型 (apple/banana 价格是 Int64)
        var price0 = df.Column("price").GetValue<long>(0);
        Assert.True(price0 == 10 || price0 == 20);
    }
    [Fact]
    public void Test_Lazy_JoinAsOf_With_TimeSpan_Tolerance()
    {
        // 场景: 股票交易匹配报价，但增加了 "2分钟" 的有效窗口
        // 如果报价太旧（超过2分钟），则认为数据失效，不予匹配
        
        // 使用 ISO 格式日期，配合 tryParseDates: true
        var tradesContent = @"time,sym,qty
2024-01-01 10:00:00,AAPL,10
2024-01-01 10:02:00,AAPL,20
2024-01-01 10:05:00,AAPL,5"; // 这个交易离最近的报价(10:01)有4分钟
        using var tradesCsv = new DisposableFile(tradesContent, ".csv");
        
        var quotesContent = @"time,sym,bid
2024-01-01 09:59:00,AAPL,150
2024-01-01 10:01:00,AAPL,151
2024-01-01 10:06:00,AAPL,152";
        using var quotesCsv = new DisposableFile(quotesContent, ".csv");

        // [关键] tryParseDates: true
        using var lfTrades = LazyFrame.ScanCsv(tradesCsv.Path, tryParseDates: true);
        using var lfQuotes = LazyFrame.ScanCsv(quotesCsv.Path, tryParseDates: true);

        // 构建 JoinAsOf
        var joinedLf = lfTrades.JoinAsOf(
            lfQuotes,
            leftOn: Col("time"),
            rightOn: Col("time"),
            
            // [核心升级] 使用 TimeSpan 强类型容差！
            tolerance: TimeSpan.FromMinutes(2), 
            
            // [核心升级] 使用枚举，拒绝魔法字符串！
            strategy: AsofStrategy.Backward,
            
            leftBy: [Col("sym")],
            rightBy: [Col("sym")]
        );

        using var df = joinedLf.Collect();
        
        // 验证结果
        Assert.Equal(3, df.Height);

        // Row 0: Trade 10:00 -> Quote 09:59 (Diff: 1m <= 2m) -> Match
        Assert.Equal(150, df.Column("bid").GetValue<long?>(0));

        // Row 1: Trade 10:02 -> Quote 10:01 (Diff: 1m <= 2m) -> Match
        Assert.Equal(151, df.Column("bid").GetValue<long?>(1));

        // Row 2: Trade 10:05 -> Quote 10:01 (Diff: 4m > 2m) -> No Match
        Assert.Null(df.Column("bid").GetValue<long?>(2));
    }
    [Fact]
    public void Test_DataFrame_To_Lazy_And_Sql()
    {
        // 1. Eager DataFrame
        var data = new[]
        {
            new { Name = "A", Val = 10 },
            new { Name = "B", Val = 20 }
        };
        using var df = DataFrame.From(data);

        // 2. 转 Lazy (新功能)
        using var lf = df.Lazy();

        // 3. 验证 Lazy 操作
        using var resDf = lf
            .Filter(Col("Val") > Lit(15))
            .Collect();

        Assert.Equal(1, resDf.Height); // Only B

        // 4. 验证原 DF 是否还活着 (关键！如果 Lazy() 没 Clone，这里会崩)
        Assert.Equal(2, df.Height); 

        // 5. 验证 SQL Context (CloneHandle 修复验证)
        using var ctx = new SqlContext();
        ctx.Register("mytable", lf); // 这里调用了 lf.CloneHandle()
        
        using var sqlRes = ctx.Execute("SELECT * FROM mytable WHERE Val < 15").Collect();
        Assert.Equal(1, sqlRes.Height); // Only A
    }
    [Fact]
    public void Test_Lazy_GroupBy_Ownership()
    {
        // 1. 数据
        var data = new[]
        {
            new { Dept = "A", Val = 10 },
            new { Dept = "A", Val = 20 },
            new { Dept = "B", Val = 30 }
        };
        using var df = DataFrame.From(data);
        using var lf = df.Lazy();

        // 2. 第一次聚合
        // GroupBy 内部 Clone 了 Handle，所以 Agg 消耗的是副本
        using var agg1 = lf.GroupBy(Col("Dept"))
                           .Agg(Col("Val").Sum().Alias("SumVal"))
                           .Collect();
        
        Assert.Equal(2, agg1.Height); // A, B

        // 3. 验证原 lf 是否还活着 (如果 GroupBy 没 Clone，这里会崩)
        // 我们做个简单的 Select 操作验证
        using var res2 = lf.Select(Col("Dept")).Collect();
        Assert.Equal(3, res2.Height);
    }
    [Fact]
    public void Test_LazyFrame_Explode()
    {
        using var s = new Series("chars", ["a,b","c"]);
        using var df = DataFrame.FromSeries(s);
        
        // 转换为 Lazy 模式
        using var lf = df.Lazy();

        using var selector = Selector.Cols("expanded");

        // 链式调用: Split -> Explode -> Collect
        // 这一步调用你的 public LazyFrame Explode(params Expr[] exprs)
        using var res = lf
            .Select(Col("chars").Str.Split(",").Alias("expanded"))
            .Explode(selector)
            .Collect();

        // 验证
        Assert.Equal(3, res.Height);
        Assert.Equal("a", res.GetValue<string>(0, "expanded"));
        Assert.Equal("b", res.GetValue<string>(1, "expanded"));
        Assert.Equal("c", res.GetValue<string>(2, "expanded"));
    }
    [Fact]
    public void TestLazySchema_ZeroParse_Introspection()
    {
        Console.WriteLine("=== LazyFrame Schema Zero-Parse Test ===");

        // 1. 准备数据
        // Schema: { "a": Int32, "b": Float64, "c": String }
        using var s1 = Series.From("a", [1, 2, 3]);
        using var s2 = Series.From("b", [1.1, 2.2, 3.3]);
        using var s3 = Series.From("c", ["apple", "banana", "cherry"]);
        using var df = DataFrame.FromSeries(s1, s2, s3);
        
        using var lf = df.Lazy();

        // 2. 获取初始 Schema
        // [Architectural Change] lf.Schema 返回 PolarsSchema 对象 (IDisposable)
        // 我们需要用 'using' 来确保 Handle 被释放
        Console.WriteLine("--- 1. Initial Schema ---");
        using var schema1 = lf.Schema; 

        // [API Update] 使用 .Length 而不是 .Count
        Assert.Equal(3, schema1.Length);
        
        // [Zero-Parse] 直接访问 Kind 枚举，没有任何字符串比较
        Assert.Equal(DataTypeKind.Int32, schema1["a"].Kind);
        Assert.Equal(DataTypeKind.Float64, schema1["b"].Kind);
        Assert.Equal(DataTypeKind.String, schema1["c"].Kind);

        // 调用新的 ToString() 实现
        Console.WriteLine(schema1.ToString()); 

        // 3. 执行 Lazy 操作并验证 Schema 变更
        // 操作：
        // - "a" Cast 为 Float64
        // - "c" Implode 聚合为 List<String>
        Console.WriteLine("\n--- 2. Modified Schema (Type Inference) ---");
        
        using var lf2 = lf.Select(
            Col("a").Cast(DataType.Float64).Alias("a_cast"),
            Col("c").Implode().Alias("c_list") // 结果应该是 List<String>
        );

        // 再次获取 Schema (Rust 会在这一步运行 Logical Plan 的类型推断)
        using var schema2 = lf2.Schema;

        // 验证 "a_cast"
        Assert.Equal(DataTypeKind.Float64, schema2["a_cast"].Kind);
        Console.WriteLine($"[Check] a_cast is Float64: {schema2["a_cast"].Kind == DataTypeKind.Float64}");

        // 4. [高光时刻] 验证嵌套类型 (Deep Introspection)
        // 这里的 c_list 是 DataTypeKind.List
        var listType = schema2["c_list"];
        Assert.Equal(DataTypeKind.List, listType.Kind);
        Console.WriteLine($"[Check] c_list is List: {listType.Kind == DataTypeKind.List}");

        // [Zero-Parse Ultimate] 获取内部类型
        // 我们不解析 "List<String>" 字符串，而是直接请求 Inner Handle
        // listType.InnerType 返回一个新的 DataType 对象
        using var innerType = listType.InnerType; 
        
        Assert.NotNull(innerType);
        Assert.Equal(DataTypeKind.String, innerType!.Kind);
        Console.WriteLine($"[Check] c_list inner type is String: {innerType.Kind == DataTypeKind.String}");

        Console.WriteLine(schema2.ToString());
        Console.WriteLine("=== SUCCESS: Validated without a single string parse! ===");
    }
    [Fact]
    public void Test_LazyFrame_Sort_FullOptions()
    {
        // 数据: 
        // val: [3, null, 1]
        // grp: [1, 1, 1]
        using var df = DataFrame.FromColumns(new 
        {
            val = new int?[] { 3, null, 1 },
            grp = new[] { 1, 1, 1 }
        });

        // Lazy Sort: 按 val 降序，但空值放最后
        // 预期顺序: [3, 1, null]
        
        using var lf = df.Lazy();
        using var sortedLf = lf.Sort(
            "val", 
            descending: true, 
            nullsLast: true // 如果 false，降序时 null 通常最大(在前)；设为 true 则强制在后
        );
        
        using var res = sortedLf.Collect();

        Assert.Equal(3, res["val"][0]);
        Assert.Equal(1, res["val"][1]);
        Assert.Null(res["val"][2]);
    }

    // 辅助打印方法
    private void PrintSchema(Dictionary<string, DataType> schema)
    {
        foreach (var kvp in schema)
        {
            var dt = kvp.Value;
            string extraInfo = dt.Kind == DataTypeKind.List 
                ? $"<Inner: {dt.InnerType?.Kind}>" 
                : "";
                
            Console.WriteLine($"Column: {kvp.Key,-10} | Kind: {dt.Kind,-10} | {dt} {extraInfo}");
        }
    }
    // 辅助方法：构造一个包含 Struct 列的 DataFrame
    // 结构: 
    //   raw: List<int> -> Array(2) -> Struct { field_0, field_1 }
    private DataFrame CreateStructDataFrame()
    {
        var data = new[] 
        { 
            new[] { 1, 2 }, 
            new[] { 10, 20 },
            new[] { 100, 200 }
        };

        var df = DataFrame.FromColumns(new { raw = data });
        
        // 构造 "my_struct" 列
        return df.Select(
            Col("raw")
                .Cast(DataType.Array(DataType.Int32, 2))
                .Array.ToStruct()
                .Alias("my_struct")
        );
    }

    [Fact]
    public void Test_LazyFrame_Unnest_With_Strings()
    {
        // 测试目标: public LazyFrame Unnest(params string[] columns)
        // 验证点: 语法糖是否正确调用了底层的 Selector 逻辑
        
        using var df = CreateStructDataFrame();

        // Action
        using var res = df.Lazy()
            .Unnest("my_struct")
            .Collect();

        // Assert
        // Struct 应该被展平成 'field_0' 和 'field_1'
        Assert.True(res.Columns.Contains("field_0"), "Result should contain field_0");
        Assert.True(res.Columns.Contains("field_1"), "Result should contain field_1");

        // 验证数据正确性 (Row 0: [1, 2])
        Assert.Equal(1, (int)res["field_0"][0]);
        Assert.Equal(2, (int)res["field_1"][0]);
        
        // 验证数据正确性 (Row 1: [10, 20])
        Assert.Equal(10, (int)res["field_0"][1]);
        Assert.Equal(20, (int)res["field_1"][1]);
    }

    [Fact]
    public void Test_LazyFrame_Unnest_With_Selector()
    {
        // 测试目标: public LazyFrame Unnest(Selector selector)
        // 验证点: 显式 Selector 传递是否正常工作，且 Handle Clone 机制未导致 Crash
        
        using var df = CreateStructDataFrame();

        // 显式构造 Selector
        using var selector = Selector.Cols("my_struct");

        // Action
        using var res = df.Lazy()
            .Unnest(selector)
            .Collect();

        // Assert
        Assert.Equal(3, res.Height); // 3行数据
        Assert.Equal(100, (int)res["field_0"][2]);
        Assert.Equal(200, (int)res["field_1"][2]);
    }

    [Fact]
    public void Test_LazyFrame_Unnest_Reuse_Selector()
    {
        // 验证你的 CloneHandle 逻辑是否允许 Selector 重用
        // 如果 API 实现里把 selector 消费了且导致 C# 端 handle 无效，这里第二次调用就会崩
        
        using var df = CreateStructDataFrame();
        using var selector = Selector.Cols("my_struct");

        // 第一次使用
        using var lf1 = df.Lazy().Unnest(selector); 
        using var res1 = lf1.Collect();
        Assert.True(res1.Columns.Contains("field_0"));

        // 第二次使用 (验证 selector 对象依然存活)
        using var lf2 = df.Lazy().Unnest(selector);
        using var res2 = lf2.Collect();
        Assert.True(res2.Columns.Contains("field_0"));
    }
    
    [Fact]
    public void Test_LazyFrame_Unnest_Multiple_Cols()
    {
        // 准备双 Struct 列数据
        var data1 = new[] { new[] { 1, 2 } };
        var data2 = new[] { new[] { 3, 4 } };
        
        using var df = DataFrame.FromColumns(new { raw1 = data1, raw2 = data2 })
            .Select(
                // s1 保持默认字段名: field_0, field_1
                Col("raw1").Cast(DataType.Array(DataType.Int32, 2)).Array.ToStruct().Alias("s1"),
                
                // s2 改名: field_0 -> other_0, field_1 -> other_1
                // [修复] 使用 RenameFields 避免 Unnest 后的列名冲突
                Col("raw2").Cast(DataType.Array(DataType.Int32, 2)).Array.ToStruct()
                    .Struct.RenameFields("other_0", "other_1") 
                    .Alias("s2")
            );

        // 同时 Unnest 两个结构体列
        using var res = df.Lazy()
            .Unnest("s1", "s2") 
            .Collect();

        // 验证列数扩展
        // s1 展开为: field_0, field_1
        // s2 展开为: other_0, other_1
        Assert.True(res.Columns.Contains("field_0"));
        Assert.True(res.Columns.Contains("other_0"));
        
        Assert.Equal(1, (int)res["field_0"][0]);
        Assert.Equal(3, (int)res["other_0"][0]);
    }
    [Fact]
    public void Test_LazyFrame_TopK_BottomK()
    {
        var data = new[] { 10, 5, 8, 100, 1 };
        using var df = DataFrame.FromColumns(new { val = data });

        // 1. 测试 TopK
        using var top = df.Lazy()
            .TopK(2, "val") // 取最大的2个: 100, 10
            .Collect();

        Assert.Equal(2, top.Height);
        var topVals = top["val"].ToArray<int>();
        Assert.Contains(100, topVals);
        Assert.Contains(10, topVals);

        // 2. 测试 BottomK
        using var bottom = df.Lazy()
            .BottomK(2, "val") // 取最小的2个: 1, 5
            .Collect();

        Assert.Equal(2, bottom.Height);
        var bottomVals = bottom["val"].ToArray<int>();
        Assert.Contains(1, bottomVals);
        Assert.Contains(5, bottomVals);
    }
    [Fact]
    public void Test_LazyFrame_Slice()
    {
        // 1. 准备 CSV: 5 行数据 (0, 1, 2, 3, 4)
        using var csv = new DisposableFile("val\n0\n1\n2\n3\n4", ".csv");
        using var lf = LazyFrame.ScanCsv(csv.Path);

        // 2. 定义切片计划: Slice(-3, 2)
        // 意思是从倒数第 3 行开始，取 2 行
        // 数据: 0, 1, [2], [3], 4
        // 倒数第三行是 "2"
        var slicedLf = lf.Slice(-3, 2);

        // 3. 执行计划 (Collect)
        using var df = slicedLf.Collect();

        // 4. 验证结果
        Assert.Equal(2, df.Height);
        
        // 验证值
        Assert.Equal(2, df["val"].GetValue<int>(0));
        Assert.Equal(3, df["val"].GetValue<int>(1));
    }
    [Fact]
    public void Test_Drop_ByColumnNames()
    {
        // 1. Arrange: 准备一个包含多列的数据
        var df = DataFrame.From(
        [
            new { Name = "Alice", Age = 25, City = "New York", Salary = 5000 },
            new { Name = "Bob", Age = 30, City = "London", Salary = 6000 }
        ]);

        // 2. Act: 使用 Lazy 模式并删除 "City" 和 "Salary" 两列
        var lf = df.Lazy();
        var res = lf.Drop("City", "Salary")
                    .Collect();

        // 3. Assert: 验证结果
        Assert.Equal(2, res.Width); // 剩下 Name 和 Age
        Assert.Contains("Name", res.ColumnNames);
        Assert.Contains("Age", res.ColumnNames);
        
        // 确保被删的列真的没了
        Assert.DoesNotContain("City", res.ColumnNames);
        Assert.DoesNotContain("Salary", res.ColumnNames);
    }

    [Fact]
    public void Test_Drop_BySelector()
    {
        // 1. Arrange
        var df = DataFrame.From(
        [
            new { A = 1, B = 2.2, C = "hello" },
            new { A = 3, B = 4.4, C = "world" }
        ]);

        // 2. Act: 使用 Selector API 删除
        // 假设我们不想看 'B' 列
        // Selector.Cols("B") 创建了一个指向 "B" 列的选择器
        var lf = df.Lazy();
        var res = lf.Drop(Selector.Cols("B"))
                    .Collect();

        // 3. Assert
        Assert.Equal(2, res.Width);
        Assert.Equal("A", res.ColumnNames[0]);
        Assert.Equal("C", res.ColumnNames[1]);
        
        // 再次确认 B 确实被丢了
        Assert.Throws<ArgumentException>(() => res["B"]);
    }
    [Fact]
    public void Test_Lazy_Unique()
    {
        var df = DataFrame.From(
        [
            new { A = 1, B = 1 },
            new { A = 1, B = 2 },
            new { A = 2, B = 3 },
            new { A = 1, B = 1 } // Duplicate of first row
        ]);

        // 1. Test Selector (Subset=[A])
        // Keep First -> 应该保留 A=1(row0), A=2(row2)
        // 注意：row1 (A=1, B=2) 因为 A 重复，且 Keep=First，可能会被丢弃（取决于 subset）
        
        var lf = df.Lazy();
        
        // Case A: Subset on "A", Keep First
        var res1 = lf.Unique(Selector.Cols("A"), UniqueKeepStrategy.First)
                     .Collect();
        
        // A=1 出现了3次，保留第一个; A=2 出现1次
        Assert.Equal(2, res1.Height); 
        
        // Case B: Subset on All (null selector), Keep None (Drop all duplicates)
        // (1,1) 重复了，会被删掉。 (1,2) 和 (2,3) 是唯一的。
        var res2 = lf.Unique(subset: null, keep: UniqueKeepStrategy.None)
                     .Collect();
                     
        Assert.Equal(2, res2.Height);
        Assert.Equal(2, (int)res2["B"][0]); 
        
        // 第 1 行应该是 (2, 3)
        Assert.Equal(3, (int)res2["B"][1]);
        
        // Case C: String overload
        var res3 = lf.Unique("A", "B").Collect();
        Assert.Equal(3, res3.Height); // (1,1), (1,2), (2,3) are kept. The last (1,1) dropped.
    }
    [Fact]
    public void Test_LazyPivot_With_Schema_Injection()
    {
        // 1. 准备原始数据 (Long Format)
        // 典型的销售记录：日期、产品、销量
        var df = DataFrame.FromColumns( new
            {
                date = new[] {"2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02"},
                product = new[] {"Apple", "Banana", "Apple", "Banana"},
                sales = new[] {100, 200, 150, 300}
            });
        var lf = df.Lazy();

        // 2. 准备 "剧透" DataFrame (onColumns)
        // 这一步告诉 Lazy 引擎：
        // "喂，虽然你还没读数据，但我告诉你，'product' 这一列透视后，
        // 会生成两列，名字分别叫 'Apple' 和 'Banana'。"
        // 注意：这里的顺序决定了结果列的顺序
        var expectedColumnsDf = new Series("product", ["Banana", "Apple"]).ToFrame(); 

        // 3. 执行 Lazy Pivot
        var pivotedLf = lf.Pivot(
            index: ["date"],          // 行索引：日期
            columns:["product"],     // 列索引：产品
            values: ["sales"],        // 值：销量
            onColumns: expectedColumnsDf,// <--- 注入 Schema
            aggregateFunction: PivotAgg.Sum, // 如果有重复，求和
            maintainOrder: true          // 保持 onColumns 的列顺序
        );

        // 4. 验证 Schema (此时还未计算，但 Schema 应该已经有了)
        // Lazy 模式下最神奇的就是这里，没跑数据，但已经知道有 Apple 和 Banana 了
        var schema = pivotedLf.Schema;
        Assert.Contains("Banana", schema.ColumnNames);
        Assert.Contains("Apple", schema.ColumnNames);
        
        // 5. 触发计算
        var result = pivotedLf.Collect();

        // 6. 验证结果
        // 应该变成了 Wide Format
        /*
        ┌────────────┬────────┬───────┐
        │ date       ┆ Banana ┆ Apple │
        │ ---        ┆ ---    ┆ ---   │
        │ str        ┆ i32    ┆ i32   │
        ╞════════════╪════════╪═══════╡
        │ 2024-01-01 ┆ 200    ┆ 100   │
        │ 2024-01-02 ┆ 300    ┆ 150   │
        └────────────┴────────┴───────┘
        */
        
        // 验证列顺序 (由 onColumns 决定，Banana 在前)
        Assert.Equal("date", result.ColumnNames[0]);
        Assert.Equal("Banana", result.ColumnNames[1]);
        Assert.Equal("Apple", result.ColumnNames[2]);

        // 验证数据
        Assert.Equal(200, result["Banana"][0]); // 01-01 Banana
        Assert.Equal(100, result["Apple"][0]);  // 01-01 Apple
        Assert.Equal(300, result["Banana"][1]); // 01-02 Banana
        Assert.Equal(150, result["Apple"][1]);  // 01-02 Apple
    }
}