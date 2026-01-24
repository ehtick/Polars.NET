namespace Polars.NET.Core.Helpers;

public static partial class ArrayHelper
{
    // =========================================================================
    // Layout Compatibility Flags
    // =========================================================================
    internal static readonly bool IsInt8LayoutCompatible;  // stride 2
    internal static readonly bool IsInt16LayoutCompatible; // stride 4
    internal static readonly bool IsInt32LayoutCompatible; // stride 8
    internal static readonly bool IsInt64LayoutCompatible; // stride 16
    // Int128 Special Flags
    internal static readonly bool IsInt128TypeA; // [Value(16), Bool(1), Pad(15)]
    internal static readonly bool IsInt128TypeB; // [Bool(1), Pad(15), Value(16)] <--- Linux .NET Default
    internal static readonly bool Int128NeedsSwap; // True if [Hi, Lo] mismatch
    /// <summary>
    /// Check Int32 Memory Layout
    /// </summary>
    static unsafe ArrayHelper()
    {
        // ---------------------------------------------------------
        // 1. Check Int8 (sbyte?) Layout
        // ---------------------------------------------------------
        // Expected Layout : [Bool(1B), Val(1B)] -> Value Offset = 1
        // ---------------------------------------------------------
        try
        {
            sbyte?[] probe = [0x7F]; // MaxValue
            fixed (sbyte?* p = probe)
            {
                byte* b = (byte*)p;
                // Bool @0, Value @1
                bool boolAt0 = *b == 1; 
                bool valAt1 = *(b + 1) == 0x7F;

                IsInt8LayoutCompatible = boolAt0 && valAt1 && (sizeof(sbyte?) == 2);
            }
        }
        catch { IsInt8LayoutCompatible = false; }
        // ---------------------------------------------------------
        // 2. Check Int16 (short?) Layout
        // ---------------------------------------------------------
        // Expected Layout: [Bool(1B), Pad(1B), Val(2B)] -> Value Offset = 2
        // ---------------------------------------------------------
        try
        {
            short?[] probe = [0x1234];
            fixed (short?* p = probe)
            {
                byte* b = (byte*)p;
                // Bool @0, Value @2
                bool boolAt0 = *b == 1;
                bool valAt2 = *(short*)(b + 2) == 0x1234;

                IsInt16LayoutCompatible = boolAt0 && valAt2 && (sizeof(short?) == 4);
            }
        }
        catch { IsInt16LayoutCompatible = false; }
        // Check Int32 Memory Layout [Bool, Pad, Pad, Pad, Int, Int, Int, Int]
        try
        {
            int?[] testProbe = [0x12345678]; // HasValue=True, Value=0x12345678
            fixed (int?* ptr = testProbe)
            {
                byte* bPtr = (byte*)ptr;
                bool hasValueAt0 = *bPtr == 1;
                bool valueAt4 = *(int*)(bPtr + 4) == 0x12345678;

                if (hasValueAt0 && valueAt4 && sizeof(int?) == 8)
                {
                    IsInt32LayoutCompatible = true;
                }
                else
                {
                    IsInt32LayoutCompatible = false;
                }
            }
        }
        catch
        {
            IsInt32LayoutCompatible = false;
        }
        // ---------------------------------------------------------
        // Check Int64 (long?) Layout
        // ---------------------------------------------------------
        // Expected Layout: [Bool(1B), Pad(7B), Val(8B)] -> Value Offset = 8
        // ---------------------------------------------------------
        try
        {
            long?[] probe = [0x1234567890ABCDEF];
            fixed (long?* p = probe)
            {
                byte* b = (byte*)p;
                // Bool @0, Value @8
                bool boolAt0 = *b == 1;
                bool valAt8 = *(long*)(b + 8) == 0x1234567890ABCDEF;

                IsInt64LayoutCompatible = boolAt0 && valAt8 && (sizeof(long?) == 16);
            }
        }
        catch { IsInt64LayoutCompatible = false; }
        // Check Int128 Layout
        try {
            Int128 val = Int128.MaxValue;
            Int128?[] probe = [val];

            fixed (Int128?* p = probe)
            {
                byte* b = (byte*)p;
                
                // Check Type A: Value @ 0, Bool @ 16
                bool valAt0 = *(Int128*)b == val;
                bool boolAt16 = *(b + 16) == 1;
                
                if (valAt0 && boolAt16)
                {
                    IsInt128TypeA = true;
                }
                else
                {
                    // Check Type B: Bool @ 0, Value @ 16
                    // Bool @ 0，Value @ 16
                    bool boolAt0 = *b == 1;
                    bool valAt16 = *(Int128*)(b + 16) == val;

                    if (boolAt0 && valAt16)
                    {
                        IsInt128TypeB = true;
                    }
                }
            }
        }
        catch { /* Ignore */ }

        // 3. Detect Int128 Internal Layout (High/Low Swap Check)
        try
        {
            // 构造一个只在低 64 位有值的数
            Int128 val = 1; 
            
            unsafe
            {
                byte* p = (byte*)&val;
                // 如果是 Little Endian (主流)，低地址 p[0] 应该是 1
                // 如果 C# 内部把 High 64 bit 放在前面，p[0] 就是 0
                
                // 我们假设 Rust 那边是标准的 Little Endian (Lo, Hi)
                // 如果 C# 这里探测出来 p[0] == 0，说明 C# 是 (Hi, Lo)，需要 Swap
                // 或者反之。
                
                // 简单粗暴的判断：我们认为 低地址存低位 是标准 (Target)
                // 如果当前环境不是这样，就需要 Swap
                bool isLittleEndian = (*p == 1);
                
                // 这里我们做一个假设：Rust 端期望的是 Little Endian
                // 如果 C# 本地不是 Little Endian，或者 C# 的 Int128 布局特殊
                // 实际场景下，通常 x64 都是 LE。
                // 如果之前的 Matrix 测试挂了，说明这里肯定有一方不一致。
                // 我们默认：如果测试挂了，就手动把这个写死为 true 来验证。
                
                // 但为了通用，我们暂时认为：如果低位不在低地址，就 Swap
                Int128NeedsSwap = !isLittleEndian; 
                
                // ★ 调试专用：如果你的机器肯定是 Little Endian 但还是挂了
                // 那可能是 Int128 的两个 ulong 字段顺序反了。
                // 这种情况下，我们需要强制 Swap。
                // 为了配合你之前的 Corruption 结论，我们这里强制开启 Swap 逻辑看看
                // Int128NeedsSwap = true; // <--- 如果自动探测失效，取消注释这行
            }
        }
        catch { }
    }
}