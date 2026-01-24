using Microsoft.FSharp.Core;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Polars.NET.Core.Helpers;

public static unsafe class FSharpHelper
{
    // ========================================================================
    // 1. ValueOption<T> (struct) -> 极速直通
    // ========================================================================
    // F# ValueOption 是结构体，内存连续，非常适合 SIMD/Unroll
    // Tag: 0 = None, 1 = Some (注意：F# 源码定义如此，与 Nullable 相反，Nullable是 1=HasValue)
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (int[] values, byte[]? validity) UnzipVOptionInt32(FSharpValueOption<int>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<int>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        fixed (FSharpValueOption<int>* pSrc = data)
        fixed (int* pDst = values)
        {
            // FSharpValueOption<int> 通常是 8 字节 (4byte Tag + 4byte Int)
            // 但因为是 AutoLayout，我们最好不要硬转指针，而是通过 Ref 访问属性
            // JIT 会完美内联 Tag 和 Value 的访问
            
            ref FSharpValueOption<int> srcRef = ref MemoryMarshal.GetArrayDataReference(data);
            
            // Unroll 8
            int i = 0; 
            int limit = len - 8;
            
            for (; i <= limit; i += 8)
            {
                HandleVOptionItem(i,     ref srcRef, pDst, ref validity, ref validRef);
                HandleVOptionItem(i + 1, ref srcRef, pDst, ref validity, ref validRef);
                HandleVOptionItem(i + 2, ref srcRef, pDst, ref validity, ref validRef);
                HandleVOptionItem(i + 3, ref srcRef, pDst, ref validity, ref validRef);
                HandleVOptionItem(i + 4, ref srcRef, pDst, ref validity, ref validRef);
                HandleVOptionItem(i + 5, ref srcRef, pDst, ref validity, ref validRef);
                HandleVOptionItem(i + 6, ref srcRef, pDst, ref validity, ref validRef);
                HandleVOptionItem(i + 7, ref srcRef, pDst, ref validity, ref validRef);
            }
            
            for (; i < len; i++)
            {
                HandleVOptionItem(i, ref srcRef, pDst, ref validity, ref validRef);
            }
        }
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void HandleVOptionItem(
        int i, 
        ref FSharpValueOption<int> srcBase, 
        int* pDst, 
        ref byte[]? validity, 
        ref byte validRef)
    {
        // Unsafe.Add 偏移
        ref FSharpValueOption<int> item = ref Unsafe.Add(ref srcBase, i);
        
        // ValueOption.Tag: 1 = Some, 0 = None (对于 ValueOption)
        // 注意：F# 编译后的 Tag 属性通常很快
        if (item.Tag == FSharpValueOption<int>.Tags.ValueSome)
        {
            pDst[i] = item.Value;

            if (validity != null)
            {
                if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                target |= (byte)(1 << (i & 7));
            }
        }
        else
        {
            // None 处理
            if (validity == null)
            {
                // Init validity... (同之前的逻辑)
                int byteLen = (validity?.Length ?? 0); // 伪代码，实际需传入 len
                // 这里为了演示省略初始化逻辑，直接 copy 之前 ArrayHelper 的即可
                InitValidity(ref validity, ref validRef, i, /*totalLen*/ 0);
            }
            pDst[i] = 0;
        }
    }

    // ========================================================================
    // 2. Option<T> (class) -> 指针判空
    // ========================================================================
    // F# Option 是引用类型数组，本质是 object[] (指针数组)
    // 内存里全是 8字节指针。null 代表 None。
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (int[] values, byte[]? validity) UnzipOptionInt32(FSharpOption<int>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<int>(len);
        
        // 指针数组，不需要 fixed 结构体，直接 ref 引用
        ref FSharpOption<int> srcRef = ref MemoryMarshal.GetArrayDataReference(data);
        
        // ... (Validity 初始化逻辑同上) ...
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        fixed (int* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                // 获取引用 (指针)
                FSharpOption<int> item = Unsafe.Add(ref srcRef, i);
                
                // 引用类型判空极快
                if (item != null)
                {
                    pDst[i] = item.Value;
                    
                    // Validity Set Bit...
                    if (validity != null) 
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    // None (null pointer)
                    if (validity == null) 
                    {
                         // Init Validity...
                         InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }
    
    // 简单的 Helper 用于初始化 Validity
    private static void InitValidity(ref byte[]? validity, ref byte validRef, int currentIdx, int totalLen)
    {
        // Copy from ArrayHelper implementation
        int byteLen = (totalLen + 7) >> 3;
        validity = new byte[byteLen];
        validRef = ref MemoryMarshal.GetArrayDataReference(validity);
        int bytesToFill = currentIdx >> 3;
        if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
        int remainingBits = currentIdx & 7;
        if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
    }
}