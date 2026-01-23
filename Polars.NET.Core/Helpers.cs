using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics.X86;
using System.Text;

namespace Polars.NET.Core
{
    public static class DurationFormatter
    {
        /// <summary>
        /// Format C# Duration to Polars duration string
        /// </summary>
        public static string ToPolarsString(TimeSpan ts)
        {
            if (ts == TimeSpan.Zero) return "0s";

            var sb = new StringBuilder();

            // Polars suffixs: ns, us, ms, s, m, h, d, w
            
            // 1. Days (d)
            if (ts.Days > 0) sb.Append($"{ts.Days}d");

            // 2. Hours (h)
            if (ts.Hours > 0) sb.Append($"{ts.Hours}h");

            // 3. Minutes (m)
            if (ts.Minutes > 0) sb.Append($"{ts.Minutes}m");

            // 4. Seconds (s)
            if (ts.Seconds > 0) sb.Append($"{ts.Seconds}s");

            // 5. Milliseconds (ms)
            if (ts.Milliseconds > 0) sb.Append($"{ts.Milliseconds}ms");

            // 6. [New in .NET 7+] Microseconds (us)
            if (ts.Microseconds > 0) sb.Append($"{ts.Microseconds}us");

            // 7. [New in .NET 7+] Nanoseconds (ns)
            if (ts.Nanoseconds > 0) sb.Append($"{ts.Nanoseconds}ns");

            return sb.ToString();
        }
        [return: System.Diagnostics.CodeAnalysis.NotNullIfNotNull(nameof(ts))]
        public static string? ToPolarsString(TimeSpan? ts)
        {
            return ts.HasValue ? ToPolarsString(ts.Value) : null;
        }
    }
    public static class PolarsExtensions
    {
        // ts.ToPolarsDuration()
        public static string ToPolarsDuration(this TimeSpan ts) 
            => DurationFormatter.ToPolarsString(ts);

        // tsNullable.ToPolarsDuration()
        public static string? ToPolarsDuration(this TimeSpan? ts) 
            => DurationFormatter.ToPolarsString(ts);
    }
    public static class ArrayHelper
    {
        /// <summary>
        /// Unzip nullable array to data array + validity BITMAP (1 bit per row)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (T[] values, byte[]? validity) UnzipNullable<T>(T?[] data, T defaultValue = default) 
            where T : struct
        {
            ArgumentNullException.ThrowIfNull(data);
            
            int len = data.Length;
            var values = GC.AllocateUninitializedArray<T>(len);
            
            byte[]? validity = null; 

            ref T? srcRef = ref MemoryMarshal.GetArrayDataReference(data);
            ref T valRef = ref MemoryMarshal.GetArrayDataReference(values);
            
            ref byte validRef = ref Unsafe.NullRef<byte>();

            for (int i = 0; i < len; i++)
            {
                ref T? v = ref Unsafe.Add(ref srcRef, i);
                
                if (v.HasValue)
                {
                    Unsafe.Add(ref valRef, i) = v.GetValueOrDefault();
                    
                    if (validity != null)
                    {
                        ref byte targetByte = ref Unsafe.Add(ref validRef, i >> 3);
                        targetByte |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        int validLen = (len + 7) >> 3;
                        validity = new byte[validLen]; 
                        validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        
                        int fullBytes = i >> 3;
                        if (fullBytes > 0)
                        {
                            validity.AsSpan(0, fullBytes).Fill(0xFF);
                        }

                        int remainingBits = i & 7;
                        if (remainingBits > 0)
                        {
                            ref byte currentByte = ref Unsafe.Add(ref validRef, fullBytes);
                            currentByte |= (byte)((1 << remainingBits) - 1);
                        }
                    }

                    Unsafe.Add(ref valRef, i) = defaultValue;
                }
            }
            return (values, validity);
        }
    }
    public static class BoolPacker
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe byte[] Pack(bool[] data)
        {
            int len = data.Length;
            int byteLen = (len + 7) >> 3;
            var buffer = GC.AllocateUninitializedArray<byte>(byteLen);

            fixed (bool* pSrc = data)
            fixed (byte* pDst = buffer)
            {
                int i = 0;

                // =========================================================
                //  通道 A: x86 AVX2 (每次吞 32 个 bool)
                //  带宽: 256-bit
                // =========================================================
                if (Avx2.IsSupported && len >= 32)
                {
                    Vector256<byte> zero = Vector256<byte>.Zero;
                    int limit = len - 32;

                    for (; i <= limit; i += 32)
                    {
                        Vector256<byte> vec = Avx2.LoadVector256((byte*)pSrc + i);
                        // False(0) -> 0xFF (MSB 1), True(1) -> 0x00 (MSB 0)
                        Vector256<byte> cmp = Avx2.CompareEqual(vec, zero);
                        // 提取 32 个 MSB
                        int mask = Avx2.MoveMask(cmp);
                        // 取反写入 (int 是 32位，正好对应 32 个 bool)
                        *(int*)(pDst + (i >> 3)) = ~mask;
                    }
                }
                // =========================================================
                //  通道 B: ARM NEON (每次吞 16 个 bool)
                //  带宽: 128-bit (Apple Silicon / Graviton)
                // =========================================================
                // 注意：这里用 elseif，因为如果支持 AVX2 优先走 AVX2 (吞吐更大)
                // 但在 ARM 机器上 Avx2.IsSupported 是 false，会自动进这里
                else if (AdvSimd.IsSupported && len >= 16)
                {
                    Vector128<byte> zero = Vector128<byte>.Zero;
                    int limit = len - 16;

                    for (; i <= limit; i += 16)
                    {
                        // 1. Load: 加载 16 个 bool
                        Vector128<byte> vec = AdvSimd.LoadVector128((byte*)pSrc + i);
                        
                        // 2. Compare: 逻辑同上
                        // False -> 0xFF, True -> 0x00
                        Vector128<byte> cmp = AdvSimd.CompareEqual(vec, zero);
                        
                        // 3. Extract MSB: .NET 8 在 ARM 上会生成优化的 NEON 指令序列
                        // 返回的是 uint，但实际上只有低 16 位有效 (对应 16 个 bool)
                        uint mask = Vector128.ExtractMostSignificantBits(cmp);
                        
                        // 4. Invert & Store
                        // ushort 是 16位 (2 bytes)，正好对应 16 个 bool
                        *(ushort*)(pDst + (i >> 3)) = (ushort)(~mask);
                    }
                }

                // =========================================================
                //  通道 C: 扫尾 (Scalar Fallback)
                // =========================================================
                // 处理剩下的 < 32 (x86) 或 < 16 (ARM) 个元素
                int byteOffset = i >> 3;
                if (byteOffset < byteLen)
                {
                    byte currentByte = 0;
                    int bitOffset = 0;
                    
                    for (; i < len; i++)
                    {
                        if (pSrc[i])
                        {
                            currentByte |= (byte)(1 << bitOffset);
                        }
                        bitOffset++;
                        
                        if (bitOffset == 8)
                        {
                            pDst[byteOffset] = currentByte;
                            byteOffset++;
                            currentByte = 0;
                            bitOffset = 0;
                        }
                    }
                    if (bitOffset > 0)
                    {
                        pDst[byteOffset] = currentByte;
                    }
                }
            }
            return buffer;
        }
        

        /// <summary>
        /// Compress bool?[] into Values Bitmask & Validity Bitmask
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (byte[] values, byte[]? validity) PackNullable(bool?[] data)
        {
            ArgumentNullException.ThrowIfNull(data);
            int len = data.Length;
            int byteLen = (len + 7) >> 3;

            var valuesBits = new byte[byteLen];
            
            byte[]? validityBits = null;

            ref bool? srcRef = ref MemoryMarshal.GetArrayDataReference(data);
            ref byte valRef = ref MemoryMarshal.GetArrayDataReference(valuesBits);
            ref byte validRef = ref Unsafe.NullRef<byte>(); 

            for (int i = 0; i < len; i++)
            {
                bool? v = Unsafe.Add(ref srcRef, i);

                if (v.HasValue)
                {
                    if (v.GetValueOrDefault())
                    {
                        ref byte targetByte = ref Unsafe.Add(ref valRef, i >> 3);
                        targetByte |= (byte)(1 << (i & 7));
                    }

                    if (validityBits != null)
                    {
                        ref byte targetValidByte = ref Unsafe.Add(ref validRef, i >> 3);
                        targetValidByte |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validityBits == null)
                    {
                        validityBits = new byte[byteLen];
                        validRef = ref MemoryMarshal.GetArrayDataReference(validityBits);

                        int fullBytes = i >> 3;
                        if (fullBytes > 0) validityBits.AsSpan(0, fullBytes).Fill(0xFF);
                        
                        int remainingBits = i & 7;
                        if (remainingBits > 0)
                        {
                            ref byte currentByte = ref Unsafe.Add(ref validRef, fullBytes);
                            currentByte |= (byte)((1 << remainingBits) - 1);
                        }
                    }
                }
            }
            return (valuesBits, validityBits);
        }
    }
}