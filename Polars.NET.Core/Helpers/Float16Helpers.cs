using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

public static partial class ArrayHelper
{
    // =================================================================================
    //  Half/Float16 Shuffle Mask (32 Bytes -> 4 items)
    //  Layout: [Bool(1), Pad(1), Val(2)] -> Total 4
    //  (Identical to Int16 mask because sizeof(Half) == sizeof(short))
    // =================================================================================
    private static readonly Vector256<byte> HalfShuffleMask = Vector256.Create(
        // Low 128: 8 Values (8 * 2 bytes) -> Extracts the 'Half' value
        (byte)2, 3,  (byte)6, 7,  (byte)10, 11, (byte)14, 15, 
        (byte)18, 19, (byte)22, 23, (byte)26, 27, (byte)30, 31,
        // High 128: 8 Bools (Offset 0) -> Extracts the 'HasValue' bool
        0, 4, 8, 12, 16, 20, 24, 28,
        // Padding
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF 
    );

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static unsafe (Half[] values, byte[]? validity) UnzipHalfSIMD(Half?[] data, Half defaultValue)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<Half>(len);
        int byteLen = (len + 7) >> 3;
        byte[]? validity = null;

        fixed (Half?* pSrc = data)
        fixed (Half* pDstVal = values)
        {
            ref byte validRef = ref Unsafe.NullRef<byte>();
            int i = 0;

            // --- SIMD 部分 ---
            if (Vector256.IsHardwareAccelerated && len >= 8)
            {
                int limit = len - 8;
                Vector256<byte> mask = HalfShuffleMask; // 需确保此变量已定义

                for (; i <= limit; i += 8)
                {
                    Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 4));
                    Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);
                    shuffled.GetLower().Store((byte*)(pDstVal + i));

                    Vector128<byte> upper = shuffled.GetUpper();
                    ulong boolsChunk = upper.AsUInt64().GetElement(0);

                    // 如果这一组含有 null
                    if (boolsChunk != 0x0101010101010101UL)
                    {
                        if (validity == null)
                        {
                            validity = new byte[byteLen];
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            
                            // 回填之前的全为 1 的字节
                            // SIMD 步进为8，所以这里 i 肯定是8的倍数，无需处理 partial byte
                            int bytesToFill = i >> 3;
                            if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                        }
                    }

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) 
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);

                        int packedByte = 0;
                        if (upper.GetElement(0) != 0) packedByte |= 1;
                        if (upper.GetElement(1) != 0) packedByte |= 2;
                        if (upper.GetElement(2) != 0) packedByte |= 4;
                        if (upper.GetElement(3) != 0) packedByte |= 8;
                        if (upper.GetElement(4) != 0) packedByte |= 16;
                        if (upper.GetElement(5) != 0) packedByte |= 32;
                        if (upper.GetElement(6) != 0) packedByte |= 64;
                        if (upper.GetElement(7) != 0) packedByte |= 128;

                        Unsafe.Add(ref validRef, i >> 3) = (byte)packedByte;

                        // 填充默认值
                        if (packedByte != 0xFF) 
                        {
                            if ((packedByte & 1) == 0) pDstVal[i] = defaultValue;
                            if ((packedByte & 2) == 0) pDstVal[i+1] = defaultValue;
                            if ((packedByte & 4) == 0) pDstVal[i+2] = defaultValue;
                            if ((packedByte & 8) == 0) pDstVal[i+3] = defaultValue;
                            if ((packedByte & 16) == 0) pDstVal[i+4] = defaultValue;
                            if ((packedByte & 32) == 0) pDstVal[i+5] = defaultValue;
                            if ((packedByte & 64) == 0) pDstVal[i+6] = defaultValue;
                            if ((packedByte & 128) == 0) pDstVal[i+7] = defaultValue;
                        }
                    }
                }
            }

            // --- 标量处理剩余部分 (SCALAR LOOP) ---
            if (i < len)
            {
                // 场景 1：目前还没有 null，尝试快速路径
                if (validity == null)
                {
                    for (; i < len; i++)
                    {
                        Half? val = pSrc[i];
                        if (val.HasValue)
                        {
                            pDstVal[i] = val.GetValueOrDefault();
                        }
                        else
                        {
                            // 发现第一个 null，初始化 validity 并跳转到处理 null 的逻辑
                            validity = new byte[byteLen];
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);

                            // 1. 回填之前的完整字节 (全1)
                            int fullBytes = i >> 3;
                            if (fullBytes > 0) 
                                Unsafe.InitBlock(ref validRef, 0xFF, (uint)fullBytes);
                            
                            // 2. [重要修复] 回填当前字节的前半部分
                            // 例如 i=10 (二进制 1010)，remainder=2。需要设置 bit 0 和 bit 1 为 1。
                            // (1 << 2) - 1 = 3 (二进制 11)
                            int remainder = i & 7;
                            if (remainder > 0)
                                Unsafe.Add(ref validRef, fullBytes) = (byte)((1 << remainder) - 1);
                            
                            goto HasNulls; // 现在可以跳到了
                        }
                    }
                    // 循环结束都没遇到 null
                    return (values, null);
                }

                // 场景 2：已经存在 null，或者刚刚创建了 validity 数组
                HasNulls:
                
                // 确保 validRef 是有效的 (如果是刚刚 goto 过来，需要刷新引用)
                if (Unsafe.IsNullRef(ref validRef) && validity != null) 
                    validRef = ref MemoryMarshal.GetArrayDataReference(validity);

                for (; i < len; i++)
                {
                    Half? val = pSrc[i];
                    if (val.HasValue)
                    {
                        pDstVal[i] = val.GetValueOrDefault();
                        // 设置位图：找到对应的字节，通过或运算(|)设置位
                        Unsafe.Add(ref validRef, i >> 3) |= (byte)(1 << (i & 7));
                    }
                    else
                    {
                        pDstVal[i] = defaultValue;
                        // 位图默认为 0，无需操作
                    }
                }
            }
        }
        return (values, validity);
    }
}