using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void UnzipScalarLoop<T>(
            ref T? baseSrcRef,      
            ref T baseValRef,       
            ref byte[]? validity,   
            int startIdx,           
            int len,                
            T defaultValue) where T : struct
        {
            ref byte validRef = ref Unsafe.NullRef<byte>();
            if (validity != null) validRef = ref MemoryMarshal.GetArrayDataReference(validity);

            for (int i = startIdx; i < len; i++)
            {
                ref T? v = ref Unsafe.Add(ref baseSrcRef, i);
                
                if (v.HasValue)
                {
                    Unsafe.Add(ref baseValRef, i) = v.GetValueOrDefault();
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
                        int byteLen = (len + 7) >> 3;
                        validity = GC.AllocateUninitializedArray<byte>(byteLen);
                        Array.Clear(validity, 0, byteLen); // 必须清零
                        validRef = ref MemoryMarshal.GetArrayDataReference(validity); // 更新引用

                        // Backfill 1s
                        int fullBytes = i >> 3;
                        if (fullBytes > 0) validity.AsSpan(0, fullBytes).Fill(0xFF);
                        int remainingBits = i & 7;
                        if (remainingBits > 0)
                        {
                            ref byte currentByte = ref Unsafe.Add(ref validRef, fullBytes);
                            currentByte |= (byte)((1 << remainingBits) - 1);
                        }
                    }
                    Unsafe.Add(ref baseValRef, i) = defaultValue;
                }
            }
        }
        // =================================================================================
        //  Int8/Byte Shuffle Mask (32 Bytes -> 4 items)
        //  Layout: [Val(1), Bool(1)] or [Bool(1), Val(1)]
        // =================================================================================
        private static readonly Vector256<byte> Int8ShuffleMask = Vector256.Create(
            // --- Output 0-15: 16 Values (提取奇数位置: 1, 3, 5...) ---
            (byte)1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31,
            
            // --- Output 16-31: 16 Bools (提取偶数位置: 0, 2, 4...) ---
            (byte)0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30
        );
        // =================================================================================
        //  Int16/Short Shuffle Mask (32 Bytes -> 4 items)
        //  Layout: [Bool(1), Pad(1), Val(2)] -> Total 4
        // =================================================================================
        private static readonly Vector256<byte> Int16ShuffleMask = Vector256.Create(
            // Low 128: 8 Values (8 * 2 bytes)
            (byte)2, 3,  (byte)6, 7,  (byte)10, 11, (byte)14, 15, 
            (byte)18, 19, (byte)22, 23, (byte)26, 27, (byte)30, 31,
            // High 128: 8 Bools (Offset 0)
            0, 4, 8, 12, 16, 20, 24, 28,
            // Padding
            0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF 
        );
        // =================================================================================
        //  Int32? Shuffle Mask (32 Bytes -> 4 items)
        //  int? Memory Layout: [HasValue(1B), Pad(3B), Value(4B)] (x64/Arm64 .NET 8)
        // =================================================================================
        private static readonly Vector256<byte> Int32ShuffleMask = Vector256.Create(
            // --- Output Bytes 0-15: The Integers (4 * 4 bytes) ---
            4, 5, 6, 7,      // Item 0 Value
            12, 13, 14, 15,  // Item 1 Value
            20, 21, 22, 23,  // Item 2 Value
            28, 29, 30, 31,  // Item 3 Value
            // --- Output Bytes 16-19: The HasValues (4 * 1 byte) ---
            0,               // Item 0 HasValue
            8,               // Item 1 HasValue
            16,              // Item 2 HasValue
            24,              // Item 3 HasValue
            // --- Rest is padding/garbage ---
            255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 
        );
        // =================================================================================
        // Int64? Shuffle Mask (32 Bytes -> 2 items)
        // Int64? usually is 16 bytes: [HasValue(1B), Pad(7B), Value(8B)]
        // =================================================================================
        private static readonly Vector256<byte> Int64ShuffleMask = Vector256.Create(
            // --- Output Bytes 0-15: extract 2 double value (2 * 8B) ---
            // Item 0 Value (offset 8-15)
            (byte)8, 9, 10, 11, 12, 13, 14, 15,
            // Item 1 Value (offset 24-31)
            (byte)24, 25, 26, 27, 28, 29, 30, 31,

            // --- Output Bytes 16-17: extract 2 HasValue ---
            (byte)0,   // Item 0 HasValue
            (byte)16,  // Item 1 HasValue

            // --- Fill ---
            0,0,0,0,0,0,0,0,0,0,0,0,0,0
        );
        // =========================================================================
        // Layout Compatibility Flags
        // =========================================================================
        private static readonly bool IsInt8LayoutCompatible;  // stride 2
        private static readonly bool IsInt16LayoutCompatible; // stride 4
        private static readonly bool IsInt32LayoutCompatible; // stride 8
        private static readonly bool IsInt64LayoutCompatible; // stride 16
        // Int128 Special Flags
        private static readonly bool IsInt128TypeA; // [Value(16), Bool(1), Pad(15)]
        private static readonly bool IsInt128TypeB; // [Bool(1), Pad(15), Value(16)] <--- Linux .NET Default
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
        }
        /// <summary>
        /// Unzip nullable array to data array + validity BITMAP (1 bit per row)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static (T[] values, byte[]? validity) UnzipNullable<T>(T?[] data, T defaultValue = default) 
            where T : struct
        {
            if (data == null || data.Length == 0) return (Array.Empty<T>(), null);
            Type t = typeof(T);

            // --- 1 Byte Values (stride 2) ---
            if (IsInt8LayoutCompatible) {
                if (t == typeof(byte))  
                    return Reinterpret<byte, T>(UnzipInt8SIMD(Unsafe.As<T?[], byte?[]>(ref data), (byte)(object)defaultValue));
                
                if (t == typeof(sbyte)) 
                    return Reinterpret<byte, T>(UnzipInt8SIMD(Unsafe.As<T?[], byte?[]>(ref data), (byte)(sbyte)(object)defaultValue));
            }

            // --- 2 Byte Values (stride 4) ---
            if (IsInt16LayoutCompatible) {
                if (t == typeof(short))  
                    return Reinterpret<short, T>(UnzipInt16SIMD(Unsafe.As<T?[], short?[]>(ref data), (short)(object)defaultValue));
                
                if (t == typeof(ushort)) 
                    return Reinterpret<short, T>(UnzipInt16SIMD(Unsafe.As<T?[], short?[]>(ref data), (short)(ushort)(object)defaultValue));
                
                if (t == typeof(Half))   
                    return Reinterpret<short, T>(UnzipInt16SIMD(Unsafe.As<T?[], short?[]>(ref data), BitConverter.HalfToInt16Bits((Half)(object)defaultValue)));
            }

            // --- 4 Byte Values (stride 8) ---
            if (IsInt32LayoutCompatible) {
                if (t == typeof(int))   
                    return Reinterpret<int, T>(UnzipInt32SIMD(Unsafe.As<T?[], int?[]>(ref data), (int)(object)defaultValue));
                
                if (t == typeof(uint))  
                    return Reinterpret<int, T>(UnzipInt32SIMD(Unsafe.As<T?[], int?[]>(ref data), (int)(uint)(object)defaultValue));
                
                if (t == typeof(float)) 
                    return Reinterpret<int, T>(UnzipInt32SIMD(Unsafe.As<T?[], int?[]>(ref data), BitConverter.SingleToInt32Bits((float)(object)defaultValue)));
            }

            // --- 8 Byte Values (stride 16) ---
            if (IsInt64LayoutCompatible) {
                if (t == typeof(long))   
                    return Reinterpret<long, T>(UnzipInt64SIMD(Unsafe.As<T?[], long?[]>(ref data), (long)(object)defaultValue));
                
                if (t == typeof(ulong))  
                    return Reinterpret<long, T>(UnzipInt64SIMD(Unsafe.As<T?[], long?[]>(ref data), (long)(ulong)(object)defaultValue));
                
                if (t == typeof(double)) 
                    return Reinterpret<long, T>(UnzipInt64SIMD(Unsafe.As<T?[], long?[]>(ref data), BitConverter.DoubleToInt64Bits((double)(object)defaultValue)));
            }

            // --- 16 Byte Values (stride 32) ---
            if (IsInt128TypeA || IsInt128TypeB) {
                if (t == typeof(Int128))  
                    return Reinterpret<Int128, T>(UnzipInt128SIMD(Unsafe.As<T?[], Int128?[]>(ref data), (Int128)(object)defaultValue));
                
                if (t == typeof(UInt128)) 
                    return Reinterpret<Int128, T>(UnzipInt128SIMD(Unsafe.As<T?[], Int128?[]>(ref data), (Int128)(UInt128)(object)defaultValue));
            }
            return UnzipGeneric(data, defaultValue);
        }
        // =================================================================================
        // SIMD Magic
        // =================================================================================

        // [Stride 2] Int8 / Byte
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe (byte[] values, byte[]? validity) UnzipInt8SIMD(byte?[] data, byte defaultValue)
        {
            int len = data.Length;
            // 1. Allocate Values (Uninitialized)
            var values = GC.AllocateUninitializedArray<byte>(len);
            int byteLen = (len + 7) >> 3;
            
            byte[]? validity = null; // Lazy Allocation

            fixed (byte?* pSrc = data)
            fixed (byte* pDstVal = values)
            {
                byte* pDstValid = null;
                int i = 0;

                // ---------------------------------------------------------
                // SIMD Loop: 16 byte? items (32 Bytes)
                // ---------------------------------------------------------
                if (Vector256.IsHardwareAccelerated && len >= 16)
                {
                    int limit = len - 16;
                    Vector256<byte> mask = Int8ShuffleMask;
                    Vector128<byte> zero = Vector128<byte>.Zero;

                    for (; i <= limit; i += 16)
                    {
                        // 1. Load 32 bytes (16 items)
                        Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 2));

                        // 2. Shuffle: 
                        // Lower 128 bits = 16 Values
                        // Upper 128 bits = 16 Bools
                        Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);

                        // 3. Store Values: Insert lower 128 bits (16 bytes) into values array
                        shuffled.GetLower().Store(pDstVal + i);

                        // 4. Validity Check
                        // Extract upper 128 bits (Bools)
                        Vector128<byte> bools = shuffled.GetUpper();

                        // Check Null (0x00)
                        // Equals(0, 0) -> 0xFF (Null)
                        // Equals(1, 0) -> 0x00 (Valid)
                        Vector128<byte> isNullVec = Vector128.Equals(bools, zero);

                        // Extract 16 bits mask (1 = Null, 0 = Valid)
                        uint nullMask = isNullVec.ExtractMostSignificantBits();

                        // If nullMask != 0，说明这 16 个里有 Null
                        if (nullMask != 0)
                        {
                            if (validity == null)
                            {
                                validity = new byte[byteLen];
                                pDstValid = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(validity));
                                
                                // Backfill: 前面 i 个全是 Valid (1)
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0) new Span<byte>(pDstValid, bytesToFill).Fill(0xFF);
                                // Int8 对齐很好，i 肯定是 16 的倍数 (byte 对齐)，
                                // 所以 i >> 3 肯定是偶数，不会有 remainingBits 需要处理。
                            }
                        }

                        if (validity != null)
                        {
                            // nullMask: 1=Null
                            // validMask: 1=Valid (取反)
                            // 注意：ExtractMostSignificantBits 返回 uint，但在处理 Vector128<byte> 时
                            // 它只用了低 16 位。
                            ushort validMask16 = (ushort)(~nullMask);

                            // 直接写入 2 个字节 (ushort)
                            // 因为 i 是 16 的倍数，i >> 3 必定是 2 的倍数，内存对齐没问题
                            *(ushort*)(pDstValid + (i >> 3)) = validMask16;

                            // Handle Default Value
                            // 如果有 Null，我们需要把对应位置的 Value 设为 Default
                            // (虽然 Polars 可能不看，但保持一致性)
                            // 这里如果是追求极致写操作，且 defaultValue 是 0，其实不用动（因为之前 Store 进去的是 0）
                            // 如果 defaultValue != 0，且 nullMask != 0，才需要修补
                            if (nullMask != 0)
                            {
                                // 这是一个慢路径，但在 SIMD 吞吐量中占比较小
                                // 手动展开或者循环修补 16 个
                                for (int k = 0; k < 16; k++)
                                {
                                    if ((nullMask & (1 << k)) != 0) // Is Null
                                    {
                                        pDstVal[i + k] = defaultValue;
                                    }
                                }
                            }
                        }
                    }
                }

                // ---------------------------------------------------------
                // Scalar Tail (Fallback)
                // ---------------------------------------------------------
                if (i < len)
                {
                    UnzipScalarLoop(
                        ref Unsafe.AsRef<byte?>(pSrc), 
                        ref Unsafe.AsRef<byte>(pDstVal), 
                        ref validity, 
                        i, 
                        len, 
                        defaultValue
                    );
                }
            }
            return (values, validity);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe (short[] values, byte[]? validity) UnzipInt16SIMD(short?[] data, short defaultValue)
        {
            int len = data.Length;
            // 1. Allocate Values (Uninitialized)
            var values = GC.AllocateUninitializedArray<short>(len);
            int byteLen = (len + 7) >> 3;
            
            byte[]? validity = null; // Lazy Allocation

            fixed (short?* pSrc = data)
            fixed (short* pDstVal = values)
            {
                byte* pDstValid = null;
                int i = 0;

                // ---------------------------------------------------------
                // SIMD Loop: 8 short? items (32 Bytes)
                // ---------------------------------------------------------
                if (Vector256.IsHardwareAccelerated && len >= 8)
                {
                    int limit = len - 8;
                    Vector256<byte> mask = Int16ShuffleMask;
                    Vector128<byte> zero = Vector128<byte>.Zero;

                    for (; i <= limit; i += 8)
                    {
                        // 1. Load 32 bytes (8 items)
                        Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 4));

                        // 2. Shuffle
                        // Lower 128: 8 Values (16 bytes)
                        // Upper 128: 8 Bools (at bytes 0-7), others garbage
                        Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);

                        // 3. Store Values (16 bytes = 8 shorts)
                        shuffled.GetLower().Store((byte*)(pDstVal + i));

                        // 4. Validity Check
                        // Extract Upper Half
                        Vector128<byte> upper = shuffled.GetUpper();

                        // Read
                        ulong boolsChunk = upper.AsUInt64().GetElement(0);

                        if (boolsChunk != 0x0101010101010101UL)
                        {
                            if (validity == null)
                            {
                                validity = new byte[byteLen];
                                pDstValid = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(validity));
                                
                                // Backfill 1s
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0) new Span<byte>(pDstValid, bytesToFill).Fill(0xFF);
                                int remainingBits = i & 7;
                                if (remainingBits > 0) *(pDstValid + bytesToFill) = (byte)((1 << remainingBits) - 1);
                            }
                        }

                        if (validity != null)
                        {
                            int packedByte = 0;
                            if (upper.GetElement(0) != 0) packedByte |= 1;
                            if (upper.GetElement(1) != 0) packedByte |= 2;
                            if (upper.GetElement(2) != 0) packedByte |= 4;
                            if (upper.GetElement(3) != 0) packedByte |= 8;
                            if (upper.GetElement(4) != 0) packedByte |= 16;
                            if (upper.GetElement(5) != 0) packedByte |= 32;
                            if (upper.GetElement(6) != 0) packedByte |= 64;
                            if (upper.GetElement(7) != 0) packedByte |= 128;

                            *(pDstValid + (i >> 3)) = (byte)packedByte;

                            // Handle Default Value for Nulls
                            if (packedByte != 0xFF) 
                            {
                                if (defaultValue != 0)
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
                }

                // ---------------------------------------------------------
                // Scalar Tail
                // ---------------------------------------------------------
                if (i < len)
                {
                    UnzipScalarLoop(
                        ref Unsafe.AsRef<short?>(pSrc), 
                        ref Unsafe.AsRef<short>(pDstVal), 
                        ref validity, 
                        i, 
                        len, 
                        defaultValue
                    );
                }
            }
            return (values, validity);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe (int[] values, byte[]? validity) UnzipInt32SIMD(int?[] data, int defaultValue)
        {
            int len = data.Length;
            // 1. Allocate Values Array without init memory
            var values = GC.AllocateUninitializedArray<int>(len);
            
            int byteLen = (len + 7) >> 3;
            byte[]? validity = null; // Lazy Allocation

            fixed (int?* pSrc = data)
            fixed (int* pDstVal = values)
            {
                byte* pDstValid = null;
                int i = 0;

                // ---------------------------------------------------------
                // SIMD Loop: 4 int? every cycle (32 Bytes)
                // ---------------------------------------------------------
                if (Vector256.IsHardwareAccelerated && len >= 4)
                {
                    int limit = len - 4;
                    Vector256<byte> mask = Int32ShuffleMask;

                    for (; i <= limit; i += 4)
                    {
                        // Load 32 bytes (4 items)
                        Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 8));

                        // Shuffle: Values to higher 16 bytes，HasValues to lower 16 bytes
                        Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);

                        // Store Values: directly write 128 bytes
                        *(Vector128<byte>*)(pDstVal + i) = shuffled.GetLower();

                        // Extract Validity Info
                        // Extract 16-19 bytes (HasValues)
                        // 1 = True, 0 = False
                        // To int: [000000B3 000000B2 000000B1 000000B0]
                        int v0 = shuffled.GetElement(16);
                        int v1 = shuffled.GetElement(17);
                        int v2 = shuffled.GetElement(18);
                        int v3 = shuffled.GetElement(19);

                        // Check：If all 1 (All Valid) , this int should be 0x01010101
                        int validityCheck = v0 | (v1 << 8) | (v2 << 16) | (v3 << 24);

                        // Null here，trigger allocation
                        if (validityCheck != 0x01010101)
                        {
                            if (validity == null)
                            {
                                validity = new byte[byteLen];
                                pDstValid = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(validity));
                                
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0) new Span<byte>(pDstValid, bytesToFill).Fill(0xFF);
                                int remainingBits = i & 7;
                                if (remainingBits > 0)
                                {
                                    *(pDstValid + bytesToFill) = (byte)((1 << remainingBits) - 1);
                                }
                            }
                        }

                        if (validity != null)
                        {
                            if (v0 != 0) SetBit(pDstValid, i);
                            if (v1 != 0) SetBit(pDstValid, i + 1);
                            if (v2 != 0) SetBit(pDstValid, i + 2);
                            if (v3 != 0) SetBit(pDstValid, i + 3);

                            if (v0 == 0) pDstVal[i] = defaultValue;
                            if (v1 == 0) pDstVal[i + 1] = defaultValue;
                            if (v2 == 0) pDstVal[i + 2] = defaultValue;
                            if (v3 == 0) pDstVal[i + 3] = defaultValue;
                        }
                    }
                }

                // ---------------------------------------------------------
                // Scalar Tail
                // ---------------------------------------------------------
                if (i < len)
                {
                    UnzipScalarLoop(
                        ref Unsafe.AsRef<int?>(pSrc), 
                        ref Unsafe.AsRef<int>(pDstVal), 
                        ref validity, 
                        i, 
                        len, 
                        defaultValue
                    );
                }
            }
            
            return (values, validity);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe (long[] values, byte[]? validity) UnzipInt64SIMD(long?[] data, long defaultValue)
        {
            int len = data.Length;
            var values = GC.AllocateUninitializedArray<long>(len); 
            int byteLen = (len + 7) >> 3;
            
            byte[]? validity = null; // Lazy Allocation

            fixed (long?* pSrc = data)
            fixed (long* pDstVal = values)
            {
                byte* pDstValid = null;
                int i = 0;

                // ---------------------------------------------------------
                // SIMD Loop: 2 items (32 Bytes)
                // ---------------------------------------------------------
                if (Vector256.IsHardwareAccelerated && len >= 2)
                {
                    int limit = len - 2;
                    // Reuse the Int64/Double Mask (same layout)
                    Vector256<byte> mask = Int64ShuffleMask;

                    for (; i <= limit; i += 2)
                    {
                        // 1. Load: Load 32 bytes
                        Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 16)); 

                        // 2. Shuffle
                        Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);

                        // 3. Store Values: Write 128 bits (16 byte = 2 longs)
                        *(Vector128<byte>*)(pDstVal + i) = shuffled.GetLower();

                        // 4. Extract Validity
                        byte b0 = shuffled.GetElement(16);
                        byte b1 = shuffled.GetElement(17);

                        // Check Null
                        if ((b0 & b1) != 1) 
                        {
                            if (validity == null)
                            {
                                validity = new byte[byteLen];
                                pDstValid = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(validity));
                                
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0) new Span<byte>(pDstValid, bytesToFill).Fill(0xFF);
                                int remainingBits = i & 7;
                                if (remainingBits > 0) *(pDstValid + bytesToFill) = (byte)((1 << remainingBits) - 1);
                            }
                        }

                        if (validity != null)
                        {
                            if (b0 != 0) SetBit(pDstValid, i);
                            if (b1 != 0) SetBit(pDstValid, i + 1);

                            // Handle Default Value
                            if (b0 == 0) pDstVal[i] = defaultValue;
                            if (b1 == 0) pDstVal[i + 1] = defaultValue;
                        }
                    }
                }

                // ---------------------------------------------------------
                // Scalar Tail
                // ---------------------------------------------------------
                if (i < len)
                {
                    UnzipScalarLoop(
                        ref Unsafe.AsRef<long?>(pSrc), 
                        ref Unsafe.AsRef<long>(pDstVal), 
                        ref validity, 
                        i, 
                        len, 
                        defaultValue
                    );
                }
            }
            return (values, validity);
        }
        // =========================================================================
        // [Stride 32] Int128 / UInt128 (32 Bytes -> 1 item)
        // Layout: [Value(16B), Bool(1B), Pad(15B)]
        // =========================================================================

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe (Int128[] values, byte[]? validity) UnzipInt128SIMD(Int128?[] data, Int128 defaultValue)
        {
            int len = data.Length;
            var values = GC.AllocateUninitializedArray<Int128>(len);
            int byteLen = (len + 7) >> 3;

            byte[]? validity = null; 

            // Micro-optimization
            bool isTypeA = IsInt128TypeA; 

            fixed (Int128?* pSrc = data)
            fixed (Int128* pDstVal = values)
            {
                byte* pDstValid = null;
                int i = 0;

                if (Vector256.IsHardwareAccelerated)
                {
                    for (; i < len; i++)
                    {
                        // 1. Load 32 bytes
                        Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 32));
                        
                        byte hasValue;

                        // 2. Get Value and Bool
                        if (isTypeA)
                        {
                            // Type A: [Value(0-15), Bool(16)]
                            // Value is Lower 128
                            raw.GetLower().Store((byte*)(pDstVal + i));
                            // Bool 在 Offset 16
                            hasValue = raw.GetElement(16);
                        }
                        else
                        {
                            // Type B: [Bool(0), Pad, Value(16-31)]
                            // Value is Upper 128
                            raw.GetUpper().Store((byte*)(pDstVal + i));
                            // Bool 在 Offset 0
                            hasValue = raw.GetElement(0);
                        }

                        // 3. Validity Check
                        if (hasValue != 1) 
                        {
                            if (validity == null)
                            {
                                validity = GC.AllocateUninitializedArray<byte>(byteLen);
                                Array.Clear(validity, 0, byteLen);
                                pDstValid = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(validity));
                                
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0) new Span<byte>(pDstValid, bytesToFill).Fill(0xFF);
                                int remainingBits = i & 7;
                                if (remainingBits > 0) *(pDstValid + bytesToFill) = (byte)((1 << remainingBits) - 1);
                            }
                        }

                        if (validity != null)
                        {
                            if (hasValue != 0) // Valid
                            {
                                *(pDstValid + (i >> 3)) |= (byte)(1 << (i & 7));
                            }
                            else // Null
                            {
                                pDstVal[i] = defaultValue;
                            }
                        }
                    }
                }

                if (i < len)
                {
                    UnzipScalarLoop(
                        ref Unsafe.AsRef<Int128?>(pSrc), 
                        ref Unsafe.AsRef<Int128>(pDstVal), 
                        ref validity, 
                        i, len, defaultValue
                    );
                }
            }
            return (values, validity);
        }
        // =================================================================================
        // Generic Fallback
        // =================================================================================

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static (T[] values, byte[]? validity) UnzipGeneric<T>(T?[] data, T defaultValue) 
            where T : struct
        {
            int len = data.Length;
            var values = GC.AllocateUninitializedArray<T>(len);
            byte[]? validity = null;

            ref T? srcRef = ref MemoryMarshal.GetArrayDataReference(data);
            ref T valRef = ref MemoryMarshal.GetArrayDataReference(values);

            UnzipScalarLoop(ref srcRef, ref valRef, ref validity, 0, len, defaultValue);

            return (values, validity);
        }
        // =========================================================================
        // Helpers
        // =========================================================================
        
        // Helper to cast result back
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static (T[], byte[]?) Reinterpret<S, T>((S[], byte[]?) res)
        {
            // Item1 is Values array (非 Nullable)，所以这里是 S[] -> T[]
            return (Unsafe.As<S[], T[]>(ref res.Item1), res.Item2);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe void SetBit(byte* basePtr, int bitIndex)
        {
            *(basePtr + (bitIndex >> 3)) |= (byte)(1 << (bitIndex & 7));
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
                // Unified SIMD Path (.NET 8+)
                // =========================================================
                if (Vector256.IsHardwareAccelerated && len >= 32)
                {
                    Vector256<byte> zero = Vector256<byte>.Zero;
                    int limit = len - 32;

                    for (; i <= limit; i += 32)
                    {
                        Vector256<byte> vec = Vector256.Load((byte*)pSrc + i);
                        Vector256<byte> cmp = Vector256.Equals(vec, zero);
                        uint mask = Vector256.ExtractMostSignificantBits(cmp);
                        *(uint*)(pDst + (i >> 3)) = ~mask;
                    }
                }
                // =========================================================
                // Scalar Path (Branchless Optimization)
                // =========================================================
                if (i < len)
                {
                    int byteOffset = i >> 3;
                    byte currentByte = 0;
                    int bitOffset = 0;

                    for (; i < len; i++)
                    {
                        currentByte |= (byte)((*(byte*)(pSrc + i)) << bitOffset);
                        
                        bitOffset++;
                        if (bitOffset == 8)
                        {
                            pDst[byteOffset] = currentByte;
                            byteOffset++;
                            currentByte = 0;
                            bitOffset = 0;
                        }
                    }
                    // Last byte
                    if (bitOffset > 0)
                    {
                        pDst[byteOffset] = currentByte;
                    }
                }
            }
            return buffer;
        }
        private static readonly Vector256<byte> DeinterleaveMask = Vector256.Create(
        (byte)0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, // Low 128: Values
        1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31       // High 128: Validity
        );
        /// <summary>
        /// Compress bool?[] into Values Bitmask & Validity Bitmask
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe (byte[] values, byte[]? validity) PackNullable(bool?[] data)
        {
            int len = data.Length;
            int byteLen = (len + 7) >> 3;

            var valuesBits = GC.AllocateUninitializedArray<byte>(byteLen);
            byte[]? validityBits = null; // Lazy Allocation

            fixed (bool?* pSrc = data)
            fixed (byte* pDstVal = valuesBits)
            {
                byte* pDstValid = null; 
                int i = 0;

                // =========================================================
                //  Unified SIMD Path (.NET 8+)
                // =========================================================
                if (Vector256.IsHardwareAccelerated && len >= 16)
                {
                    Vector256<byte> zero = Vector256<byte>.Zero;
                    int limit = len - 16;

                    for (; i <= limit; i += 16)
                    {
                        // 1. Load: load 16 bool? 
                        // Memory: [Val, HasVal, Val, HasVal ...] 
                        Vector256<byte> vec = Vector256.Load((byte*)pSrc + (i * 2));

                        // 2. Shuffle: 
                        // Result: [Val0...Val15 | HasVal0...HasVal15]
                        Vector256<byte> shuffled = Vector256.Shuffle(vec, DeinterleaveMask);

                        // 3. Compare: Check whether True (0x01)
                        // True(01) == 0 -> 00 (MSB 0)
                        // False/Null(00) == 0 -> FF (MSB 1)
                        Vector256<byte> cmp = Vector256.Equals(shuffled, zero);

                        // 4. Extract MSB: compress to 32-bit uint
                        uint mask = Vector256.ExtractMostSignificantBits(cmp);

                        // 5. Invert
                        uint finalMask = ~mask;

                        // 6. Split: 
                        // Warning：if Values and Validity is reverted，shuffle below two lines
                        ushort valMask16 = (ushort)(finalMask & 0xFFFF);       // Values
                        ushort validMask16 = (ushort)(finalMask >>> 16);       // Validity

                        // 7. Store Values
                        *(ushort*)(pDstVal + (i >> 3)) = valMask16;

                        // 8. Handle Lazy Validity
                        if (validMask16 != 0xFFFF)
                        {
                            if (validityBits == null)
                            {
                                validityBits = GC.AllocateUninitializedArray<byte>(byteLen);
                                
                                pDstValid = (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(validityBits));

                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0)
                                {
                                    new Span<byte>(pDstValid, bytesToFill).Fill(0xFF);
                                }
                            }
                        }

                        if (validityBits != null)
                        {
                            *(ushort*)(pDstValid + (i >> 3)) = validMask16;
                        }
                    }
                }

                // =========================================================
                //  Scalar Path 
                // =========================================================
                if (i < len)
                {
                    ref bool? srcRef = ref Unsafe.AsRef<bool?>(pSrc);
                    ref byte valRef = ref Unsafe.AsRef<byte>(pDstVal);
                    ref byte validRef = ref Unsafe.NullRef<byte>();

                    if (validityBits != null)
                    {
                        validRef = ref MemoryMarshal.GetArrayDataReference(validityBits);
                    }

                    for (; i < len; i++)
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
                }
            }

            return (valuesBits, validityBits);
        }
    }
}