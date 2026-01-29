using Microsoft.FSharp.Core;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Polars.NET.Core.Helpers;

public static unsafe class FSharpHelper
{
    // ========================================================================
    // 1. ValueOption<T> (struct) -> Values + Validity
    // ========================================================================
    // F# ValueOption is struct. Generic type + Unmanaged
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (T[] values, byte[]? validity) UnzipValueOption<T>(FSharpValueOption<T>[] data)
        where T : unmanaged
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<T>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        ref FSharpValueOption<T> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (T* pDst = values)
        {
            // Unroll 8
            int i = 0; 
            int limit = len - 8;
            int totalLen = len; 

            for (; i <= limit; i += 8)
            {
                HandleVOptionItem(i,     ref srcRef, pDst, ref validity, ref validRef, totalLen);
                HandleVOptionItem(i + 1, ref srcRef, pDst, ref validity, ref validRef, totalLen);
                HandleVOptionItem(i + 2, ref srcRef, pDst, ref validity, ref validRef, totalLen);
                HandleVOptionItem(i + 3, ref srcRef, pDst, ref validity, ref validRef, totalLen);
                HandleVOptionItem(i + 4, ref srcRef, pDst, ref validity, ref validRef, totalLen);
                HandleVOptionItem(i + 5, ref srcRef, pDst, ref validity, ref validRef, totalLen);
                HandleVOptionItem(i + 6, ref srcRef, pDst, ref validity, ref validRef, totalLen);
                HandleVOptionItem(i + 7, ref srcRef, pDst, ref validity, ref validRef, totalLen);
            }
            
            for (; i < len; i++)
            {
                HandleVOptionItem(i, ref srcRef, pDst, ref validity, ref validRef, totalLen);
            }
        }
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void HandleVOptionItem<T>(
        int i, 
        ref FSharpValueOption<T> srcBase, 
        T* pDst, 
        ref byte[]? validity, 
        ref byte validRef,
        int totalLen) where T : unmanaged
    {
        ref FSharpValueOption<T> item = ref Unsafe.Add(ref srcBase, i);
        
        // F# ValueOption Tag: 1 = ValueSome, 0 = ValueNone
        if (item.Tag == FSharpValueOption<T>.Tags.ValueSome)
        {
            pDst[i] = item.Value;

            if (validity != null)
            {
                // Unsafe.IsNullRef check is fast
                if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                
                ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                target |= (byte)(1 << (i & 7));
            }
        }
        else
        {
            if (validity == null)
            {
                InitValidity(ref validity, ref validRef, i, totalLen);
            }
            
            pDst[i] = default;
        }
    }

    // ========================================================================
    // 2. Option<T> (class) -> Values + Validity
    // ========================================================================
    // F# Option is ref type (class) Array is object[] (ptr array)。
    
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (T[] values, byte[]? validity) UnzipOption<T>(FSharpOption<T>[] data)
        where T : unmanaged
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<T>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();
        
        // Get ptr array ref
        ref FSharpOption<T> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (T* pDst = values)
        {
            int i = 0;
            int limit = len - 4;

            for (; i <= limit; i += 4)
            {
                HandleOptionItem(i,     ref srcRef, pDst, ref validity, ref validRef, len);
                HandleOptionItem(i + 1, ref srcRef, pDst, ref validity, ref validRef, len);
                HandleOptionItem(i + 2, ref srcRef, pDst, ref validity, ref validRef, len);
                HandleOptionItem(i + 3, ref srcRef, pDst, ref validity, ref validRef, len);
            }

            for (; i < len; i++)
            {
                HandleOptionItem(i, ref srcRef, pDst, ref validity, ref validRef, len);
            }
        }
        return (values, validity);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void HandleOptionItem<T>(
        int i,
        ref FSharpOption<T> srcBase,
        T* pDst,
        ref byte[]? validity,
        ref byte validRef,
        int totalLen) where T : unmanaged
    {
        // Get ref
        FSharpOption<T> item = Unsafe.Add(ref srcBase, i);
        
        // item != null means Some
        if (item != null)
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
            // None (null pointer)
            if (validity == null) 
            {
                 InitValidity(ref validity, ref validRef, i, totalLen);
            }
            pDst[i] = default;
        }
    }
    
    // ========================================================================
    // Validity Initialization Helper (Backfill Logic)
    // ========================================================================
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void InitValidity(ref byte[]? validity, ref byte validRef, int currentIdx, int totalLen)
    {
        // Calc Bitmap Total Bytes
        int byteLen = (totalLen + 7) >> 3;
        validity = new byte[byteLen];
        validRef = ref MemoryMarshal.GetArrayDataReference(validity);
        
        // Fill all Valid byte (0xFF)
        int fullBytes = currentIdx >> 3;
        if (fullBytes > 0)
        {
            Unsafe.InitBlock(ref validRef, 0xFF, (uint)fullBytes);
        }

        // Fill valid bit before current byte 
        // Index 18. 18 & 7 = 2 (010). means this byte 0, 1 bite is Valid， 2 is Null
        // Mask = (1 << 2) - 1 = 3 (00000011)
        int remainingBits = currentIdx & 7;
        if (remainingBits > 0)
        {
            ref byte target = ref Unsafe.Add(ref validRef, fullBytes);
            target = (byte)((1 << remainingBits) - 1);
        }
    }
    // ========================================================================
    // 3. Boolean Packing (Option/VOption -> Bitmaps directly)
    // ========================================================================

    /// <summary>
    /// Pack FSharpValueOption<bool>[] directly to Arrow Bitmaps (Values + Validity)
    /// Zero-Allocation intermediate.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (byte[] values, byte[]? validity) PackValueOptionBool(FSharpValueOption<bool>[] data)
    {
        int len = data.Length;
        int byteLen = (len + 7) >> 3;
        
        var values = new byte[byteLen];
        byte[]? validity = null; // Lazy init

        // Pointers
        ref byte valuesRef = ref MemoryMarshal.GetArrayDataReference(values);
        ref byte validRef = ref Unsafe.NullRef<byte>(); // Placeholder
        ref FSharpValueOption<bool> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        for (int i = 0; i < len; i++)
        {
            // Get Item
            ref FSharpValueOption<bool> item = ref Unsafe.Add(ref srcRef, i);

            // Tag: 1 = Some, 0 = None
            if (item.Tag == FSharpValueOption<bool>.Tags.ValueSome)
            {
                // Is True?
                if (item.Value)
                {
                    // Set Value Bit
                    ref byte targetVal = ref Unsafe.Add(ref valuesRef, i >> 3);
                    targetVal |= (byte)(1 << (i & 7));
                }
                
                // Set Validity Bit
                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte targetValid = ref Unsafe.Add(ref validRef, i >> 3);
                    targetValid |= (byte)(1 << (i & 7));
                }
            }
            else
            {
                // None -> Handle Validity Initialization
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
                // Value bit remains 0 (default)
            }
        }
        return (values, validity);
    }

    /// <summary>
    /// Pack FSharpOption<bool>[] directly to Arrow Bitmaps
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (byte[] values, byte[]? validity) PackOptionBool(FSharpOption<bool>[] data)
    {
        int len = data.Length;
        int byteLen = (len + 7) >> 3;

        var values = new byte[byteLen];
        byte[]? validity = null;

        ref byte valuesRef = ref MemoryMarshal.GetArrayDataReference(values);
        ref byte validRef = ref Unsafe.NullRef<byte>();
        
        // Src is array of references (pointers)
        ref FSharpOption<bool> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        for (int i = 0; i < len; i++)
        {
            FSharpOption<bool> item = Unsafe.Add(ref srcRef, i);

            if (item != null) // Some
            {
                if (item.Value)
                {
                    ref byte targetVal = ref Unsafe.Add(ref valuesRef, i >> 3);
                    targetVal |= (byte)(1 << (i & 7));
                }

                if (validity != null)
                {
                    if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                    ref byte targetValid = ref Unsafe.Add(ref validRef, i >> 3);
                    targetValid |= (byte)(1 << (i & 7));
                }
            }
            else // None
            {
                if (validity == null)
                {
                    InitValidity(ref validity, ref validRef, i, len);
                }
            }
        }
        return (values, validity);
    }

    // ========================================================================
    // 4. String Unwrapping (Option/VOption -> string[])
    // ========================================================================

    /// <summary>
    /// Unwrap FSharpOption<string>[] -> string[] (null for None)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static string?[] UnwrapOptionString(FSharpOption<string>[] data)
    {
        int len = data.Length;
        var result = new string?[len];
        
        ref FSharpOption<string> srcRef = ref MemoryMarshal.GetArrayDataReference(data);
        ref string? dstRef = ref MemoryMarshal.GetArrayDataReference(result);

        for (int i = 0; i < len; i++)
        {
            // Load pointer
            FSharpOption<string> item = Unsafe.Add(ref srcRef, i);
            
            // If item is not null, take Value, else null
            // F# Option<RefType> implementation: Value property holds the ref
            Unsafe.Add(ref dstRef, i) = item?.Value;
        }
        return result;
    }

    /// <summary>
    /// Unwrap FSharpValueOption<string>[] -> string[] (null for None)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static string?[] UnwrapValueOptionString(FSharpValueOption<string>[] data)
    {
        int len = data.Length;
        var result = new string?[len];

        ref FSharpValueOption<string> srcRef = ref MemoryMarshal.GetArrayDataReference(data);
        ref string? dstRef = ref MemoryMarshal.GetArrayDataReference(result);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<string> item = ref Unsafe.Add(ref srcRef, i);
            
            if (item.Tag == FSharpValueOption<string>.Tags.ValueSome)
            {
                Unsafe.Add(ref dstRef, i) = item.Value;
            }
            else
            {
                Unsafe.Add(ref dstRef, i) = null;
            }
        }
        return result;
    }
    /// <summary>
    /// Pack FSharpOption<DateTime>[] directly to Microseconds[]
    /// (Zero-Allocation, Pointer-based Unzip)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe (long[] values, byte[]? validity) UnzipOptionDateTimeToUs(FSharpOption<DateTime>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long mask = 0x3FFFFFFFFFFFFFFF; 
        long epoch = 621355968000000000; 

        // Ref to the start of the pointer array
        ref FSharpOption<DateTime> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (long* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                // Load reference (pointer) directly
                // FSharpOption<T> is a class, so this is effectively reading an object reference
                FSharpOption<DateTime> item = Unsafe.Add(ref srcRef, i);

                // Check for null (None)
                if (item != null)
                {
                    // Some: Extract Value
                    long ticks = item.Value.Ticks & mask;
                    pDst[i] = (ticks - epoch) / 10;

                    // Update Validity Bitmap
                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    // None
                    if (validity == null)
                    {
                        // Initialize Validity (Backfill 1s for previous valid items)
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }
    /// <summary>
    /// Pack FSharpValueOption<DateTime>[] directly to Microseconds[]
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipValueOptionDateTimeToUs(FSharpValueOption<DateTime>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long mask = 0x3FFFFFFFFFFFFFFF; 
        long epoch = 621355968000000000; 

        // FSharpValueOption<DateTime> layout?
        // It is struct: [Tag(4), DateTime(8)]? No.
        // F# ValueOption<T> layout depends on T.
        // For DateTime (struct), it likely packs nicely but alignment matters.
        // It's safer to use ref Unsafe.Add logic rather than raw pointers unless verified layout.
        
        ref FSharpValueOption<DateTime> srcRef = ref MemoryMarshal.GetArrayDataReference(data);
        fixed (long* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                ref FSharpValueOption<DateTime> item = ref Unsafe.Add(ref srcRef, i);
                
                if (item.Tag == FSharpValueOption<DateTime>.Tags.ValueSome)
                {
                    // Logic from ArrayHelper
                    long ticks = item.Value.Ticks & mask;
                    pDst[i] = (ticks - epoch) / 10;
                    
                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }
    // Inside FSharpHelper.cs

    // ========================================================================
    // DateOnly Support (Option/VOption -> Int32[] Days)
    // ========================================================================
    
    /// <summary>
    /// Pack FSharpOption<DateOnly>[] directly to Int32[] (Days since 1970-01-01)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (int[] values, byte[]? validity) UnzipOptionDateOnlyToInt32(FSharpOption<DateOnly>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<int>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        int epochShift = 719162; 

        ref FSharpOption<DateOnly> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (int* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                FSharpOption<DateOnly> item = Unsafe.Add(ref srcRef, i);

                if (item != null)
                {
                    // Item.Value is DateOnly struct
                    // .DayNumber is int
                    pDst[i] = item.Value.DayNumber - epochShift;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }

    /// <summary>
    /// Pack FSharpValueOption<DateOnly>[] directly to Int32[] (Days since 1970-01-01)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe (int[] values, byte[]? validity) UnzipValueOptionDateOnlyToInt32(FSharpValueOption<DateOnly>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<int>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        int epochShift = 719162;

        ref FSharpValueOption<DateOnly> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (int* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                ref FSharpValueOption<DateOnly> item = ref Unsafe.Add(ref srcRef, i);

                if (item.Tag == FSharpValueOption<DateOnly>.Tags.ValueSome)
                {
                    pDst[i] = item.Value.DayNumber - epochShift;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }
    // Inside FSharpHelper.cs

    // ========================================================================
    // DateTimeOffset Support (Option/VOption -> UTC Microseconds)
    // ========================================================================

    /// <summary>
    /// Pack FSharpOption<DateTimeOffset>[] directly to UTC Microseconds[]
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe (long[] values, byte[]? validity) UnzipOptionDateTimeOffsetToUs(FSharpOption<DateTimeOffset>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long epoch = 621355968000000000; 

        ref FSharpOption<DateTimeOffset> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (long* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                // Load reference (pointer)
                FSharpOption<DateTimeOffset> item = Unsafe.Add(ref srcRef, i);

                if (item != null)
                {
                    // Access .Value.UtcTicks directly
                    long utcTicks = item.Value.UtcTicks;
                    pDst[i] = (utcTicks - epoch) / 10;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }

    /// <summary>
    /// Pack FSharpValueOption<DateTimeOffset>[] directly to UTC Microseconds[]
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static unsafe (long[] values, byte[]? validity) UnzipValueOptionDateTimeOffsetToUs(FSharpValueOption<DateTimeOffset>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long epoch = 621355968000000000; 

        ref FSharpValueOption<DateTimeOffset> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (long* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                ref FSharpValueOption<DateTimeOffset> item = ref Unsafe.Add(ref srcRef, i);

                if (item.Tag == FSharpValueOption<DateTimeOffset>.Tags.ValueSome)
                {
                    long utcTicks = item.Value.UtcTicks;
                    pDst[i] = (utcTicks - epoch) / 10;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }

    // ========================================================================
    // TimeOnly Support (Option/VOption -> Int64[] Nanoseconds)
    // ========================================================================

    /// <summary>
    /// Pack FSharpOption<TimeOnly>[] directly to Int64[] (Nanoseconds)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipOptionTimeOnlyToNs(FSharpOption<TimeOnly>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long multiplier = 100; // Ticks(100ns) -> ns

        ref FSharpOption<TimeOnly> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (long* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                FSharpOption<TimeOnly> item = Unsafe.Add(ref srcRef, i);

                if (item != null)
                {
                    // item.Value is TimeOnly struct -> .Ticks is long
                    pDst[i] = item.Value.Ticks * multiplier;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }

    /// <summary>
    /// Pack FSharpValueOption<TimeOnly>[] directly to Int64[] (Nanoseconds)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipValueOptionTimeOnlyToNs(FSharpValueOption<TimeOnly>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        long multiplier = 100;

        ref FSharpValueOption<TimeOnly> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (long* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                ref FSharpValueOption<TimeOnly> item = ref Unsafe.Add(ref srcRef, i);

                if (item.Tag == FSharpValueOption<TimeOnly>.Tags.ValueSome)
                {
                    pDst[i] = item.Value.Ticks * multiplier;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }
    // Inside FSharpHelper.cs

    // ========================================================================
    // TimeSpan Support (Option/VOption -> Int64[] Microseconds)
    // ========================================================================

    /// <summary>
    /// Pack FSharpOption<TimeSpan>[] directly to Int64[] (Microseconds)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipOptionTimeSpanToUs(FSharpOption<TimeSpan>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        // Ref to pointer array
        ref FSharpOption<TimeSpan> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (long* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                FSharpOption<TimeSpan> item = Unsafe.Add(ref srcRef, i);

                if (item != null)
                {
                    // item.Value.Ticks / 10 -> us
                    pDst[i] = item.Value.Ticks / 10;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }

    /// <summary>
    /// Pack FSharpValueOption<TimeSpan>[] directly to Int64[] (Microseconds)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static (long[] values, byte[]? validity) UnzipValueOptionTimeSpanToUs(FSharpValueOption<TimeSpan>[] data)
    {
        int len = data.Length;
        var values = GC.AllocateUninitializedArray<long>(len);
        byte[]? validity = null;
        ref byte validRef = ref Unsafe.NullRef<byte>();

        ref FSharpValueOption<TimeSpan> srcRef = ref MemoryMarshal.GetArrayDataReference(data);

        fixed (long* pDst = values)
        {
            for (int i = 0; i < len; i++)
            {
                ref FSharpValueOption<TimeSpan> item = ref Unsafe.Add(ref srcRef, i);

                if (item.Tag == FSharpValueOption<TimeSpan>.Tags.ValueSome)
                {
                    pDst[i] = item.Value.Ticks / 10;

                    if (validity != null)
                    {
                        if (Unsafe.IsNullRef(ref validRef)) validRef = ref MemoryMarshal.GetArrayDataReference(validity);
                        ref byte target = ref Unsafe.Add(ref validRef, i >> 3);
                        target |= (byte)(1 << (i & 7));
                    }
                }
                else
                {
                    if (validity == null)
                    {
                        InitValidity(ref validity, ref validRef, i, len);
                    }
                    pDst[i] = 0;
                }
            }
        }
        return (values, validity);
    }
    // ========================================================================
    // Decimal Support (Option/VOption -> decimal?[])
    // ========================================================================

    /// <summary>
    /// Unwrap FSharpOption<decimal>[] -> decimal?[] (Fast Path)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static decimal?[] UnwrapOptionDecimal(FSharpOption<decimal>[] data)
    {
        int len = data.Length;
        var result = new decimal?[len];

        ref FSharpOption<decimal> srcRef = ref MemoryMarshal.GetArrayDataReference(data);
        ref decimal? dstRef = ref MemoryMarshal.GetArrayDataReference(result);

        for (int i = 0; i < len; i++)
        {
            FSharpOption<decimal> item = Unsafe.Add(ref srcRef, i);

            Unsafe.Add(ref dstRef, i) = item?.Value;
        }
        return result;
    }

    /// <summary>
    /// Unwrap FSharpValueOption<decimal>[] -> decimal?[] (Fast Path)
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public static decimal?[] UnwrapValueOptionDecimal(FSharpValueOption<decimal>[] data)
    {
        int len = data.Length;
        var result = new decimal?[len];

        ref FSharpValueOption<decimal> srcRef = ref MemoryMarshal.GetArrayDataReference(data);
        ref decimal? dstRef = ref MemoryMarshal.GetArrayDataReference(result);

        for (int i = 0; i < len; i++)
        {
            ref FSharpValueOption<decimal> item = ref Unsafe.Add(ref srcRef, i);

            if (item.Tag == FSharpValueOption<decimal>.Tags.ValueSome)
            {
                Unsafe.Add(ref dstRef, i) = item.Value;
            }
            else
            {
                Unsafe.Add(ref dstRef, i) = null;
            }
        }
        return result;
    }
}