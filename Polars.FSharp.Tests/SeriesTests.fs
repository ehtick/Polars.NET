namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open Apache.Arrow
open System

type ``Series Tests`` () =

    [<Fact>]
    member _.``Series: Create Int32 with Nulls`` () =
        let data = [Some 1; None; Some 3; Some 42]
        use s = Series.create("nums", data)
        
        Assert.Equal("nums", s.Name)
        Assert.Equal(4L, s.Length)
        
        // 转 Arrow 验证
        let arrow = s.ToArrow() :?> Int32Array
        Assert.Equal(4, arrow.Length)
        Assert.Equal(1, arrow.GetValue(0).Value)
        Assert.True(arrow.IsNull 1) // Null Check
        Assert.Equal(3, arrow.GetValue(2).Value)

    [<Fact>]
    member _.``Series: Create Strings with Nulls`` () =
        let data = [Some "hello"; None; Some "world"]
        use s = Series.create("strings", data)
        
        let arrow = s.ToArrow() 
        // Polars 0.50+ 默认 StringViewArray，或者 LargeStringArray
        // 这里做个类型匹配
        match arrow with
        | :? StringViewArray as sa ->
            Assert.Equal("hello", sa.GetString 0)
            Assert.True(sa.IsNull 1)
            Assert.Equal("world", sa.GetString 2)
        | :? StringArray as sa -> // Fallback logic
            Assert.Equal("hello", sa.GetString 0)
            Assert.True(sa.IsNull 1)
        | _ -> failwithf "Unexpected arrow type: %s" (arrow.GetType().Name)

    [<Fact>]
    member _.``Series: Rename`` () =
        use s = Series.create("a", [1;2])
        Assert.Equal("a", s.Name)
        
        s.Rename("b") |> ignore
        Assert.Equal("b", s.Name)

    [<Fact>]
    member _.``Series: Float with Nulls`` () =
        let data = [Some 1.5; None; Some 3.14]
        use s = Series.create("floats", data)
        
        let arrow = s.ToArrow() :?> DoubleArray
        Assert.Equal(1.5, arrow.GetValue(0).Value)
        Assert.True(arrow.IsNull(1))
        Assert.Equal(3.14, arrow.GetValue(2).Value)
    [<Fact>]
    member _.``Interop: DataFrame <-> Series`` () =
        // 1. 创建 DataFrame
        use csv = new TempCsv "name,age\nalice,10\nbob,20"
        let df = DataFrame.ReadCsv csv.Path
                
        // 2. 获取 Series (ByName)
        use sName = df.Column "name"
        Assert.Equal("name", sName.Name)
        Assert.Equal(2L, sName.Length)
        sName |> pl.showSeries |> ignore
        // 3. 获取 Series (ByIndex)
        use sAge = df.Column 1
        Assert.Equal("age", sAge.Name)

        // 4. 索引器语法
        use sAge2 = df.[1]
        Assert.Equal("age", sAge2.Name)

        // 5. Series -> DataFrame
        let dfNew = sAge.ToFrame()
        Assert.Equal(1L, dfNew.Width)
        Assert.Equal(2L, dfNew.Rows)
        Assert.Equal("age", dfNew.ColumnNames.[0])
    [<Fact>]
    member _.``Series: Cast to Categorical`` () =
        // 1. 创建字符串 Series (高重复)
        let data = ["apple"; "banana"; "apple"; "apple"; "banana"]
        use s = Series.create("fruits", data)
        
        // 2. 转换为 Categorical
        use sCat = s.Cast DataType.Categorical
        
        // 3. 验证 Arrow 类型
        let arrow = sCat.ToArrow()
        
        // [修复] 使用 DictionaryArray 基类
        Assert.IsAssignableFrom<Apache.Arrow.DictionaryArray> arrow |> ignore
        
        // 进一步验证内部结构
        let dictArr = arrow :?> Apache.Arrow.DictionaryArray
        
        // 验证索引类型 (Polars 通常使用 UInt32 作为物理索引)
        // 注意：Indices 也是一个 IArrowArray
        let indices = dictArr.Indices
        Assert.IsAssignableFrom<Apache.Arrow.UInt32Array> indices |> ignore
        
        // 验证字典值 (应该是去重后的字符串)
        let values = dictArr.Dictionary
        // 可能是 StringArray 或 StringViewArray (取决于 Polars 兼容性设置)
        Assert.True(values :? Apache.Arrow.StringArray || values :? Apache.Arrow.StringViewArray)
        
        // 验证值内容 (apple, banana)
        Assert.Equal(2, values.Length)

    [<Fact>]
    member _.``Series: Cast to Decimal (From String)`` () =
        // 1. 使用字符串源数据，保证精度
        let data = ["1.23"; "4.56"; "7.89"]
        use s = Series.create("money", data)
        
        // 2. String -> Decimal (Precision=10, Scale=2)
        // Polars 解析字符串 "4.56" -> 456 (int128) -> 正确
        use sDec = s.Cast(DataType.Decimal(Some 10,Some 2))
        
        // 3. 验证
        let arrow = sDec.ToArrow()
        let decArr = arrow :?> Decimal128Array
        
        Assert.Equal(1.23m, decArr.GetValue(0).Value)
        Assert.Equal(4.56m, decArr.GetValue(1).Value) // 完美通过
        Assert.Equal(7.89m, decArr.GetValue(2).Value)
    [<Fact>]
    member _.``Series: Create Decimal (High Performance)`` () =
        // 数据: 1.23, 4.56
        // 我们指定 Scale = 2
        let data = [1.23m; 4.56m; 7.89m] 
        
        // 使用新加的 create 方法
        use s = Series.create("money", data, 2)
        
        // 验证
        let arrow = s.ToArrow() :?> Apache.Arrow.Decimal128Array
        
        // 这次 4.56m 进去，出来的必定是 4.56m
        // 因为我们在 C# 端做了 * 100 操作：4.56m * 100m = 456m -> (Int128)456
        // 绝对没有浮点数中间商赚差价
        Assert.Equal(4.56m, arrow.GetValue(1).Value)
    [<Fact>]
    member _.``Scalar Access: Series & DataFrame`` () =
        // Series 验证
        use s = Series.create("d", [1.23m; 4.56m], 2)
        Assert.Equal(Some 1.23m, s.Decimal 0)
        Assert.Equal(Some 4.56m, s.Decimal 1)
        
        // DataFrame 验证 (Redirect)
        use df = DataFrame.create [s]
        Assert.Equal(Some 1.23m, df.Decimal("d", 0))
    [<Fact>]
    member _.``Series: IsNull / IsNotNull`` () =
        // 数据: 1, null, 3
        let s = Series.create("a", [Some 1; None; Some 3])

        // 1. IsNull -> [false, true, false]
        let maskNull = s.IsNull()
        Assert.Equal("bool", maskNull.DtypeStr)
        Assert.Equal(Some false, maskNull.Bool 0)
        Assert.Equal(Some true, maskNull.Bool 1)

        // 2. IsNotNull -> [true, false, true]
        let maskNotNull = s.IsNotNull()
        Assert.Equal(Some true, maskNotNull.Bool 0)
        Assert.Equal(Some false, maskNotNull.Bool 1)
    [<Fact>]
    member _.``Series: Dt Extraction`` () =
        // 2023-01-01 10:30:00
        let dt = DateTime(2023, 1, 1, 10, 30, 0)
        let s = Series.create("dates", [dt])

        // 验证 Year
        let sYear = s.Dt.Year()
        Assert.Equal(2023, sYear.GetValue<int> 0)

        // 验证 Month
        let sMonth = s.Dt.Month()
        Assert.Equal(1, sMonth.GetValue<int> 0)

        // 验证 Hour
        let sHour = s.Dt.Hour()
        Assert.Equal(10, sHour.GetValue<int> 0)

    [<Fact>]
    member _.``Series: Dt Manipulation (Offset & Truncate)`` () =
        let dt = DateTime(2023, 1, 1, 10, 30, 45)
        let s = Series.create("dates", [dt])

        // Truncate to 1h -> 10:00:00
        let sTrunc = s.Dt.Truncate("1h")
        let valTrunc = sTrunc.GetValue<DateTime>(0)
        Assert.Equal(DateTime(2023, 1, 1, 10, 0, 0), valTrunc)

        // Offset by 1d -> 2023-01-02
        let sOffset = s.Dt.OffsetBy("1d")
        let valOffset = sOffset.GetValue<DateTime>(0)
        Assert.Equal(DateTime(2023, 1, 2, 10, 30, 45), valOffset)

    [<Fact>]
    member _.``Series: Dt Business Days`` () =
        // 2023-01-06 (周五)
        let d = DateOnly(2023, 1, 6)
        let s = Series.create("dates", [d])

        // Add 1 Business Day -> Mon 2023-01-09
        let sNextBiz = s.Dt.AddBusinessDays(1)
        let valNext = sNextBiz.GetValue<DateOnly>(0)
        
        Assert.Equal(DateOnly(2023, 1, 9), valNext)

        // Is Business Day
        let sIsBiz = s.Dt.IsBusinessDay()
        Assert.True(sIsBiz.GetValue<bool>(0))
    [<Fact>]
    member _.``Series: Str Basic Ops (Case, Slice, Len)`` () =
        let s = Series.create("txt", ["Hello"; "World"; "Polars"])

        // ToUpper
        let sUpper = s.Str.ToUpper()
        Assert.Equal("HELLO", sUpper.GetValue<string>(0))

        // Slice (Offset 1, Len 2) -> "el", "or", "ol"
        let sSlice = s.Str.Slice(1L, 2UL)
        Assert.Equal("el", sSlice.GetValue<string> 0)
        Assert.Equal("or", sSlice.GetValue<string> 1)
        
        // Len
        let sLen = s.Str.Len()
        Assert.Equal(5u, sLen.GetValue<uint32> 0) // Polars len returns uint32

    [<Fact>]
    member _.``Series: Str Regex & Replace`` () =
        let s = Series.create("txt", ["a1b"; "c2d"])

        // Replace Digit with * (Regex)
        let sRep = s.Str.ReplaceAll("\d", "*", useRegex=true)
        Assert.Equal("a*b", sRep.GetValue<string> 0)
        Assert.Equal("c*d", sRep.GetValue<string> 1)

        // Contains "b"
        let sHasB = s.Str.Contains "b"
        Assert.True(sHasB.GetValue<bool> 0)
        Assert.False(sHasB.GetValue<bool> 1)

    [<Fact>]
    member _.``Series: Str Split (Returns List)`` () =
        let s = Series.create("csv", ["a,b,c"; "x,y"])
        
        // Split -> List<String>
        let sList = s.Str.Split(",")
        
        // 验证 Row 0: ["a", "b", "c"]
        // 利用我们之前加的 GetList 方法
        let l0 = sList.GetList<string>(0)
        Assert.Equal<string list>(["a"; "b"; "c"], l0)

        // 验证 Row 1: ["x", "y"]
        let l1 = sList.GetList<string>(1)
        Assert.Equal<string list>(["x"; "y"], l1)

    [<Fact>]
    member _.``Series: Str Parsing (ToDate)`` () =
        let s = Series.create("dates", ["2023-01-01"; "2023-12-31"])
        
        // Parse String to Date
        let sDate = s.Str.ToDate("%Y-%m-%d")
        
        // 验证类型是否变成了 Date (DateOnly)
        // 这里的 GetValue 应该能自动拆箱
        Assert.Equal(DateOnly(2023, 1, 1), sDate.GetValue<DateOnly> 0)
        Assert.Equal(DateOnly(2023, 12, 31), sDate.GetValue<DateOnly> 1)

    [<Fact>]
    member _.``Series: Str Strip & Trim`` () =
        let s = Series.create("txt", ["  hello  "; "__world__"])

        // Strip Whitespace
        let sTrim = s.Str.Strip()
        Assert.Equal("hello", sTrim.GetValue<string> 0)

        // Strip custom chars
        let sStripCustom = s.Str.Strip("_")
        Assert.Equal("world", sStripCustom.GetValue<string> 1)
    [<Fact>]
    member _.``Series: List Basic Ops`` () =
        // Data: [[1, 2], [3]]
        let data = [
            {| Vals = [1; 2] |}
            {| Vals = [3] |}
        ]
        // 必须 Cast 为 List (Variable Length)
        // 假设 DataFrame.ofRecords 默认生成 List<int>
        let df = DataFrame.ofRecords data
        let s = df.Column "Vals"

        // Len
        let sLen = s.List.Len()
        Assert.Equal(2u, sLen.GetValue<uint32> 0)
        Assert.Equal(1u, sLen.GetValue<uint32> 1)

        // Sum
        let sSum = s.List.Sum()
        Assert.Equal(3, sSum.GetValue<int> 0) // 1+2
        Assert.Equal(3, sSum.GetValue<int> 1) // 3

    [<Fact>]
    member _.``Series: List Concat (Binary Op)`` () =
        // s1: [1], [2]
        let s1 = Series.create("A", [1; 2])
        // s2: [10], [20]
        let s2 = Series.create("B", [10; 20])

        // Concat: A + B -> [[1, 10], [2, 20]]
        // 这里 s1, s2 是 Int 类型，ConcatList 会自动把它们视为 Scalar 放入 List
        // 或者如果它们已经是 List，则合并。
        // 根据 pl.concat_list 行为，如果输入是 Scalar，它会构造 List。
        let sRes = s1.List.Concat(s2)
        
        // 验证 Row 0: [1, 10]
        let l0 = sRes.GetList<int>(0)
        Assert.Equal<int list>([1; 10], l0)

    [<Fact>]
    member _.``Series: List Concat Name Collision`` () =
        // 测试 ApplyBinaryExpr 的改名逻辑
        let s1 = Series.create("SameName", [1])
        let s2 = Series.create("SameName", [99])

        // 两个 Series 名字一样，直接放在一个 DF 会报错
        // ApplyBinaryExpr 应该自动处理
        let sRes = s1.List.Concat s2
        
        // 验证: [1, 99]
        let l0 = sRes.GetList<int> 0
        Assert.Equal<int list>([1; 99], l0)
    [<Fact>]
    member _.``Series: Array Aggregations`` () =
        // 1. 准备数据并转换为 Array 类型
        let data = [
            {| Vals = [1; 2; 3] |}
            {| Vals = [4; 5; 6] |}
        ]
        // 必须先在 DataFrame 层面 Cast 为 Array(Int32, 3)
        let df = 
            DataFrame.ofRecords(data)
                .WithColumns([
                    pl.col("Vals").Cast(DataType.Array(DataType.Int32, 3UL))
                ])
        
        // 2. 提取 Series (此时它已经是 Array 类型了)
        let s = df.Column "Vals"

        // 3. 测试 Sum
        // Row 0: 1+2+3=6
        // Row 1: 4+5+6=15
        let sSum = s.Array.Sum()
        Assert.Equal(6, sSum.GetValue<int> 0)
        Assert.Equal(15, sSum.GetValue<int> 1)

        // 4. 测试 Min
        let sMin = s.Array.Min()
        Assert.Equal(1, sMin.GetValue<int> 0)
        Assert.Equal(4, sMin.GetValue<int> 1)

    [<Fact>]
    member _.``Series: Array Operations (Sort & Get)`` () =
        let data = [
            {| Vals = [3; 1; 2] |}
        ]
        let df = 
            DataFrame.ofRecords(data)
                .WithColumns([
                    pl.col("Vals").Cast(DataType.Array(DataType.Int32, 3UL))
                ])
        
        let s = df.Column "Vals"

        // 1. Sort -> [1, 2, 3]
        // Series.Array.Sort 返回的是一个新的 Series
        let sSorted = s.Array.Sort()
        
        // 验证: 取出 List 对比
        let l0 = sSorted.GetList<int>(0)
        Assert.Equal<int list>([1; 2; 3], l0)

        // 2. Get(Index=1) -> 1 (原始数据是 [3, 1, 2])
        let sGet = s.Array.Get(1)
        Assert.Equal(1, sGet.GetValue<int> 0)

    [<Fact>]
    member _.``Series: Array Join (String)`` () =
        let data = [
            {| Vals = ["a"; "b"; "c"] |}
        ]
        let df = 
            DataFrame.ofRecords(data)
                .WithColumns([
                    pl.col("Vals").Cast(DataType.Array(DataType.String, 3UL))
                ])
        
        let s = df.Column "Vals"

        // Join -> "a-b-c"
        let sJoined = s.Array.Join "-"
        Assert.Equal("a-b-c", sJoined.GetValue<string> 0)
    [<Fact>]
    member _.``Series: Struct Field Access (Heterogeneous)`` () =
        // 1. 准备异构数据
        let data = [
            {| ID = 1; Name = "Alice" |}
            {| ID = 2; Name = "Bob"   |}
        ]
        let df = DataFrame.ofRecords data

        // 2. 使用 pl.asStruct 构造 Struct Series
        // 这次我们可以优雅地把 ID(Int) 和 Name(String) 打包在一起
        let dfStruct = 
            df.Select([
                pl.asStruct([ pl.col "ID"; pl.col "Name" ]).Alias "User"
            ])
        
        let s = dfStruct.Column "User" // Struct<ID: i32, Name: str>

        // 3. 测试 Field (ByName)
        // 访问 Int 字段
        let fId = s.Struct.Field "ID"
        Assert.Equal(1, fId.GetValue<int> 0)

        // 访问 String 字段 (这在之前的 Array hack 里测不了！)
        let fName = s.Struct.Field "Name"
        Assert.Equal("Alice", fName.GetValue<string> 0)

        // 4. 测试 Field (ByIndex)
        let fIndex1 = s.Struct.Field 1 // Index 1 is Name
        Assert.Equal("Bob", fIndex1.GetValue<string> 1)

    [<Fact>]
    member _.``Series: Struct Rename & Json`` () =
        // 1. 构造 Struct
        let df = 
            DataFrame.ofRecords([ {| A = 10; B = 20 |} ])
                .Select([
                    pl.asStruct([ pl.col "A"; pl.col "B" ]).Alias "Data"
                ])
        let s = df.Column "Data"

        // 2. Rename Fields
        // A -> X, B -> Y
        let sRenamed = s.Struct.RenameFields ["X"; "Y"]
        
        // 验证
        let valX = sRenamed.Struct.Field("X")
        Assert.Equal(10, valX.GetValue<int> 0)

        // 3. Json Encode
        let sJson = sRenamed.Struct.JsonEncode()
        let jsonStr = sJson.GetValue<string> 0
        
        // 验证 JSON 结构
        // 这里的顺序取决于 Polars 内部实现，通常是保留顺序
        Assert.Contains("X", jsonStr)
        Assert.Contains("10", jsonStr)
        Assert.Contains("Y", jsonStr)
        Assert.Contains("20", jsonStr)
    [<Fact>]
    member _.``Series: Trig & Hyperbolic`` () =
        // 准备数据: [0, PI/2, PI]
        let data = [0.0; System.Math.PI / 2.0; System.Math.PI]
        let s = Series.create("angle", data)

        // 1. 测试 Sin
        // Sin(0)=0, Sin(PI/2)=1, Sin(PI)~0
        let sSin = s.Sin()
        Assert.Equal(0.0, sSin.GetValue<double> 0, 5)
        Assert.Equal(1.0, sSin.GetValue<double> 1, 5)

        // 2. 测试往返: ArcSin(Sin(x))
        // 注意 ArcSin 定义域在 [-1, 1]，值域 [-PI/2, PI/2]
        // 所以只有前两个点能完美还原
        let sRoundTrip = sSin.ArcSin()
        Assert.Equal(0.0, sRoundTrip.GetValue<double> 0, 5)
        Assert.Equal(System.Math.PI / 2.0, sRoundTrip.GetValue<double> 1, 5)

        // 3. 测试 Cosh (双曲余弦)
        // Cosh(0) = 1
        let sCosh = s.Cosh()
        Assert.Equal(1.0, sCosh.GetValue<double> 0, 5)