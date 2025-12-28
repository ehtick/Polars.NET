using Polars.NET.Core;

namespace Polars.CSharp;
/// <summary>
/// Intermediate builder for LazyGroupBy operations.
/// Holds the LazyFrame handle (ownership transferred to this builder) and grouping keys.
/// </summary>
public class LazyGroupBy : IDisposable
{
    private readonly LazyFrameHandle _lfHandle;
    
    // [改进] 持有 Handle 而不是 Expr 对象，且是我们自己 Clone 来的
    // 这样我们就拥有了这些 Handle 的所有权，不受外部影响
    private readonly ExprHandle[] _ownedKeyHandles; 
    
    private bool _disposed;

    internal LazyGroupBy(LazyFrameHandle lfHandle, Expr[] keys)
    {
        _lfHandle = lfHandle;

        // [改进] 构造时立即 Clone，锁定生命周期
        // 这样即使用户传进来的 keys 下一秒就被 Dispose 了，我们这里也是安全的
        _ownedKeyHandles = new ExprHandle[keys.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            _ownedKeyHandles[i] = PolarsWrapper.CloneExpr(keys[i].Handle);
        }
    }

    /// <summary>
    /// Apply aggregations to the group.
    /// </summary>
    public LazyFrame Agg(params Expr[] aggs)
    {
        if (_disposed) throw new ObjectDisposedException(nameof(LazyGroupBy));

        // 1. 准备 Agg Handles (Clone)
        // Agg 表达式通常是临时的，这里 Clone 是为了传给 Rust 消费
        var aggHandles = new ExprHandle[aggs.Length];
        for (int i = 0; i < aggs.Length; i++)
        {
            aggHandles[i] = PolarsWrapper.CloneExpr(aggs[i].Handle);
        }

        // 2. 调用 Wrapper
        // 【最稳妥的做法】：再 Clone 一份给 Rust 消费
        var keysForRust = new ExprHandle[_ownedKeyHandles.Length];
        for(int i=0; i<_ownedKeyHandles.Length; i++)
        {
            keysForRust[i] = PolarsWrapper.CloneExpr(_ownedKeyHandles[i]);
        }
        
        // _lfHandle 也是 Clone 来的，可以安全交给 Rust 消费
        var resHandle = PolarsWrapper.LazyGroupByAgg(_lfHandle, keysForRust, aggHandles);
        
        return new LazyFrame(resHandle);
    }
    /// <summary>
    /// Dispose all used handles
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            // 释放我们持有的 Key Handles
            foreach (var h in _ownedKeyHandles)
            {
                if (h != null && !h.IsInvalid) h.Dispose();
            }
            
            // _lfHandle 在 Agg 中被消费了吗？
            // 按照你的逻辑：NativeBindings.pl_lazy_groupby_agg 会消耗这个 handle。
            // 如果用户创建了 LazyGroupBy 但没调用 Agg 就 Dispose 了，我们需要释放 _lfHandle
            // 这需要引入一个标记，或者让 Rust 端只 Borrow LF。
            // 但 LazyFrame 操作通常是链式的 consuming。
            
            // 如果 Agg 还没调用，我们需要释放 _lfHandle
            // 如果 Agg 调用了，handle 所有权已经移交，SetHandleAsInvalid 应该被调用过
            if (!_lfHandle.IsClosed) 
            {
                 _lfHandle.Dispose();
            }

            _disposed = true;
        }
    }
}

/// <summary>
/// Intermediate builder for LazyDynamicGroupBy operations.
/// </summary>
public class LazyDynamicGroupBy
{
    private readonly LazyFrameHandle _lfHandle;
    private readonly Expr[] _keys;
    private readonly string _indexColumn;
    private readonly string _every;
    private readonly string _period;
    private readonly string _offset;
    private readonly Label _label; // [修改]
    private readonly StartBy _startBy;
    private readonly bool _includeBoundaries;
    private readonly ClosedWindow _closedWindow;

    internal LazyDynamicGroupBy(
        LazyFrameHandle lfHandle,
        string indexColumn,
        string every,
        string period,
        string offset,
        Expr[] keys,
        Label label, // [修改]
        bool includeBoundaries,
        ClosedWindow closedWindow,
        StartBy startBy)
    {
        _lfHandle = lfHandle;
        _indexColumn = indexColumn;
        _every = every;
        _period = period;
        _offset = offset;
        _keys = keys;
        _label = label;
        _includeBoundaries = includeBoundaries;
        _closedWindow = closedWindow;
        _startBy = startBy;
    }
    /// <summary>
    /// Apply aggregations to the group.
    /// This consumes the internal LazyFrame handle.
    /// </summary>
    public LazyFrame Agg(params Expr[] aggs)
    {
        // 1. 准备 Key Handles (Clone)
        var keyHandles = _keys.Select(k => PolarsWrapper.CloneExpr(k.Handle)).ToArray();
        
        // 2. 准备 Agg Handles (Clone)
        var aggHandles = aggs.Select(a => PolarsWrapper.CloneExpr(a.Handle)).ToArray();
        var newHandle = PolarsWrapper.LazyGroupByDynamic(
                _lfHandle,
                _indexColumn,
                _every,
                _period,
                _offset,
                _label.ToNative(),
                _includeBoundaries,
                _closedWindow.ToNative(),
                _startBy.ToNative(),
                keyHandles,
                aggHandles
            );

            // 3. 返回新对象
            return new LazyFrame(newHandle);
    }
}