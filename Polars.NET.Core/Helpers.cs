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
            // 1. Init validRef
            ref byte validRef = ref Unsafe.NullRef<byte>();
            if (validity != null) 
                validRef = ref MemoryMarshal.GetArrayDataReference(validity);

            for (int i = startIdx; i < len; i++)
            {
                ref T? v = ref Unsafe.Add(ref baseSrcRef, i);

                if (v.HasValue)
                {
                    // 1. Write Value
                    Unsafe.Add(ref baseValRef, i) = v.GetValueOrDefault();

                    // 2. Input Validity
                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) 
                            validRef = ref MemoryMarshal.GetArrayDataReference(validity);

                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        int byteLen = (len + 7) >> 3;
                        validity = new byte[byteLen]; 
                        validRef = ref MemoryMarshal.GetArrayDataReference(validity);

                        int bytesToFill = i >> 3;
                        if (bytesToFill > 0) 
                        {
                            Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                        }
                        int remainingBits = i & 7;
                        if (remainingBits > 0)
                        {
                            Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
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
            // --- Output 0-15: 16 Values  1, 3, 5...) ---
            (byte)1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31,
            
            // --- Output 16-31: 16 Bools  0, 2, 4...) ---
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
            
            byte[]? validity = null; 

            fixed (byte?* pSrc = data)
            fixed (byte* pDstVal = values)
            {
                ref byte validRef = ref Unsafe.NullRef<byte>();
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

                        // 3. Store Values
                        shuffled.GetLower().Store(pDstVal + i);

                        // 4. Validity Check
                        Vector128<byte> bools = shuffled.GetUpper();

                        // Check Null (0x00)
                        // Equals(0, 0) -> 0xFF (Null)
                        // Equals(1, 0) -> 0x00 (Valid)
                        Vector128<byte> isNullVec = Vector128.Equals(bools, zero);

                        // Extract 16 bits mask (1 = Null, 0 = Valid)
                        uint nullMask = isNullVec.ExtractMostSignificantBits();

                        if (nullMask != 0)
                        {
                            if (validity == null)
                            {
                                validity = GC.AllocateUninitializedArray<byte>(byteLen);
                                Array.Clear(validity, 0, byteLen); 
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                                
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0)
                                {
                                    Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                                }
                            }
                        }

                        if (validity != null)
                        {
                            if (Unsafe.IsNullRef(ref validRef)) 
                            {
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            }
                            ushort validMask16 = (ushort)(~nullMask);

                            Unsafe.WriteUnaligned(
                                ref Unsafe.Add(ref validRef, i >> 3), 
                                validMask16
                            );

                            // Handle Default Value
                            if (nullMask != 0)
                            {
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
            var values = GC.AllocateUninitializedArray<short>(len);
            int byteLen = (len + 7) >> 3;
            byte[]? validity = null; 

            fixed (short?* pSrc = data)
            fixed (short* pDstVal = values)
            {
                ref byte validRef = ref Unsafe.NullRef<byte>();
                int i = 0;

                if (Vector256.IsHardwareAccelerated && len >= 8)
                {
                    int limit = len - 8;
                    Vector256<byte> mask = Int16ShuffleMask;

                    for (; i <= limit; i += 8)
                    {
                        Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 4));
                        Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);
                        shuffled.GetLower().Store((byte*)(pDstVal + i));

                        Vector128<byte> upper = shuffled.GetUpper();
                        ulong boolsChunk = upper.AsUInt64().GetElement(0);

                        if (boolsChunk != 0x0101010101010101UL)
                        {
                            if (validity == null)
                            {
                                validity = new byte[byteLen];
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                                
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                            }
                        }

                        if (validity != null)
                        {
                            if (Unsafe.IsNullRef(ref validRef)) 
                            {
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            }

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
                
                if (i < len)
                {
                    UnzipScalarLoop(
                        ref Unsafe.AsRef<short?>(pSrc), 
                        ref Unsafe.AsRef<short>(pDstVal), 
                        ref validity, i, len, defaultValue
                    );
                }
            }
            return (values, validity);
        }
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe (int[] values, byte[]? validity) UnzipInt32SIMD(int?[] data, int defaultValue)
        {
            int len = data.Length;
            var values = GC.AllocateUninitializedArray<int>(len);
            int byteLen = (len + 7) >> 3;
            byte[]? validity = null; 
            
            fixed (int?* pSrc = data)
            fixed (int* pDstVal = values)
            {
                ref byte validRef = ref Unsafe.NullRef<byte>();
                int i = 0;

                if (Vector256.IsHardwareAccelerated && len >= 4)
                {
                    int limit = len - 4;
                    Vector256<byte> mask = Int32ShuffleMask;

                    for (; i <= limit; i += 4)
                    {
                        Vector256<byte> raw = Vector256.Load((byte*)pSrc + (i * 8));
                        Vector256<byte> shuffled = Vector256.Shuffle(raw, mask);
                        *(Vector128<byte>*)(pDstVal + i) = shuffled.GetLower();

                        int v0 = shuffled.GetElement(16);
                        int v1 = shuffled.GetElement(17);
                        int v2 = shuffled.GetElement(18);
                        int v3 = shuffled.GetElement(19);
                        int validityCheck = v0 | (v1 << 8) | (v2 << 16) | (v3 << 24);

                        if (validityCheck != 0x01010101) 
                        {
                            if (validity == null)
                            {
                                validity = new byte[byteLen];
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                                
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                                int remainingBits = i & 7;
                                if (remainingBits > 0) Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                            }
                        }

                        if (validity != null)
                        {
                            if (Unsafe.IsNullRef(ref validRef)) 
                            {
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            }

                            if (v0 != 0) SetBitRef(ref validRef, i);
                            if (v1 != 0) SetBitRef(ref validRef, i + 1);
                            if (v2 != 0) SetBitRef(ref validRef, i + 2);
                            if (v3 != 0) SetBitRef(ref validRef, i + 3);

                            if (v0 == 0) pDstVal[i] = defaultValue;
                            if (v1 == 0) pDstVal[i + 1] = defaultValue;
                            if (v2 == 0) pDstVal[i + 2] = defaultValue;
                            if (v3 == 0) pDstVal[i + 3] = defaultValue;
                        }
                    }
                }

                if (i < len)
                {
                    // Fallback
                    UnzipScalarLoop(
                        ref Unsafe.AsRef<int?>(pSrc), 
                        ref Unsafe.AsRef<int>(pDstVal), 
                        ref validity, 
                        i, len, defaultValue
                    );
                }
            }
            return (values, validity);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static unsafe (long[] values, byte[]? validity) UnzipInt64SIMD(long?[] data, long defaultValue)
        {
            int len = data.Length;
            // 1. Allocate Values (Uninitialized)
            var values = GC.AllocateUninitializedArray<long>(len); 
            int byteLen = (len + 7) >> 3;
            
            byte[]? validity = null; 
            
            // 【Key Change 1】GC Safe ref
            

            fixed (long?* pSrc = data)
            fixed (long* pDstVal = values)
            {
                ref byte validRef = ref Unsafe.NullRef<byte>();
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
                                // 【Key Change 2】Allocate & Clear & Init
                                validity = new byte[byteLen];
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                                
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                                int remainingBits = i & 7;
                                if (remainingBits > 0)
                                {
                                    Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                                }
                            }
                        }

                        if (validity != null)
                        {
                            if (Unsafe.IsNullRef(ref validRef)) 
                            {
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            }
                            // 【Key Change 3】Use SetBitRef
                            if (b0 != 0) SetBitRef(ref validRef, i);
                            if (b1 != 0) SetBitRef(ref validRef, i + 1);

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
                        ref validity, // auto sync
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
            // 1. Allocate Values (Uninitialized)
            var values = GC.AllocateUninitializedArray<Int128>(len);
            int byteLen = (len + 7) >> 3;

            byte[]? validity = null; 

            // Cache static field to local for speed
            bool isTypeA = IsInt128TypeA; 

            fixed (Int128?* pSrc = data)
            fixed (Int128* pDstVal = values)
            {
                ref byte validRef = ref Unsafe.NullRef<byte>();
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
                            raw.GetLower().Store((byte*)(pDstVal + i));
                            hasValue = raw.GetElement(16);
                        }
                        else
                        {
                            // Type B: [Bool(0), Pad, Value(16-31)]
                            raw.GetUpper().Store((byte*)(pDstVal + i));
                            hasValue = raw.GetElement(0);
                        }

                        // 3. Validity Check
                        if (hasValue != 1) 
                        {
                            if (validity == null)
                            {
                                validity = new byte[byteLen]; 
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                                
                                // Backfill 1s
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0) Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                                int remainingBits = i & 7;
                                if (remainingBits > 0) 
                                {
                                    Unsafe.Add(ref validRef, bytesToFill) = (byte)((1 << remainingBits) - 1);
                                }
                            }
                        }

                        if (validity != null)
                        {
                            if (Unsafe.IsNullRef(ref validRef)) 
                            {
                                validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                            }
                            if (hasValue != 0) // Valid
                            {
                                ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                                target |= (byte)(1 << (i & 7));
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
            => (Unsafe.As<S[], T[]>(ref res.Item1), res.Item2);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void SetBitRef(ref byte baseRef, int bitIndex)
        {
            ref byte target = ref Unsafe.Add(ref baseRef, bitIndex >> 3);
            target |= (byte)(1 << (bitIndex & 7));
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
        /// Compress bool?[] into Values Bitmask & Validity Bitmask.
        /// Fixed: Uses ref byte for validity array to ensure GC safety.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static unsafe (byte[] values, byte[]? validity) PackNullable(bool?[] data)
        {
            int len = data.Length;
            int byteLen = (len + 7) >> 3;

            var valuesBits = GC.AllocateUninitializedArray<byte>(byteLen);
            byte[]? validityBits = null; 

            ref byte validRef = ref Unsafe.NullRef<byte>();

            fixed (bool?* pSrc = data)
            fixed (byte* pDstVal = valuesBits)
            {
                int i = 0;

                // =========================================================
                // 1. Unified SIMD Path (.NET 8+) 
                // =========================================================
                if (Vector256.IsHardwareAccelerated && len >= 16)
                {
                    Vector256<byte> zero = Vector256<byte>.Zero;
                    int limit = len - 16;

                    for (; i <= limit; i += 16)
                    {
                        Vector256<byte> vec = Vector256.Load((byte*)pSrc + (i * 2));
                        Vector256<byte> shuffled = Vector256.Shuffle(vec, DeinterleaveMask);
                        Vector256<byte> cmp = Vector256.Equals(shuffled, zero);
                        uint mask = Vector256.ExtractMostSignificantBits(cmp);
                        uint finalMask = ~mask;

                        ushort valMask16 = (ushort)(finalMask & 0xFFFF);
                        ushort validMask16 = (ushort)(finalMask >>> 16);

                        *(ushort*)(pDstVal + (i >> 3)) = valMask16;

                        // Lazy Allocation Logic
                        if (validMask16 != 0xFFFF)
                        {
                            if (validityBits == null)
                            {
                                validityBits = GC.AllocateUninitializedArray<byte>(byteLen);
                                validRef = ref MemoryMarshal.GetArrayDataReference(validityBits);

                                // Backfill 1s (All valid until now)
                                int bytesToFill = i >> 3;
                                if (bytesToFill > 0)
                                {
                                    Unsafe.InitBlock(ref validRef, 0xFF, (uint)bytesToFill);
                                }
                            }
                        }

                        if (validityBits != null)
                        {
                            Unsafe.WriteUnaligned(
                                ref Unsafe.Add(ref validRef, i >> 3), 
                                validMask16
                            );
                        }
                    }
                }

                // =========================================================
                // 2. Optimized Scalar Path
                // =========================================================
                if (i < len)
                {
                    if (validityBits != null && Unsafe.IsNullRef(ref validRef))
                    {
                         validRef = ref MemoryMarshal.GetArrayDataReference(validityBits);
                    }

                    int byteOffset = i >> 3;
                    byte curValByte = 0;
                    byte curValidByte = 0; 
                    int bitPos = 0;

                    ref bool? srcRef = ref Unsafe.AsRef<bool?>(pSrc);

                    for (; i < len; i++)
                    {
                        bool? v = Unsafe.Add(ref srcRef, i);

                        if (v.HasValue)
                        {
                            curValidByte |= (byte)(1 << bitPos);
                            
                            if (v.GetValueOrDefault())
                            {
                                curValByte |= (byte)(1 << bitPos);
                            }
                        }
                        else
                        {
                            if (validityBits == null)
                            {
                                validityBits = GC.AllocateUninitializedArray<byte>(byteLen);
                                validRef = ref MemoryMarshal.GetArrayDataReference(validityBits);
                                
                                if (byteOffset > 0)
                                {
                                    Unsafe.InitBlock(ref validRef, 0xFF, (uint)byteOffset);
                                }
                            }
                        }

                        bitPos++;

                        if (bitPos == 8)
                        {
                            // Flush register to mem
                            pDstVal[byteOffset] = curValByte;
                            
                            if (validityBits != null)
                            {
                                Unsafe.Add(ref validRef, byteOffset) = curValidByte;
                            }

                            byteOffset++;
                            curValByte = 0;
                            curValidByte = 0;
                            bitPos = 0;
                        }
                    }

                    if (bitPos > 0)
                    {
                        pDstVal[byteOffset] = curValByte;
                        if (validityBits != null)
                        {
                            Unsafe.Add(ref validRef, byteOffset) = curValidByte;
                        }
                    }
                }
            }

            return (valuesBits, validityBits);
        }
    }
}