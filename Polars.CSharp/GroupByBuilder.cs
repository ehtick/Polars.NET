using Polars.NET.Core;

namespace Polars.CSharp;
/// <summary>
/// Builder for GroupByAggs
/// </summary>
public class GroupByBuilder
{
    private readonly DataFrame _df;
    private readonly Expr[] _by;

    internal GroupByBuilder(DataFrame df, Expr[] by)
    {
        _df = df;
        _by = by;
    }
    /// <summary>
    /// Aggregate with specified expressions
    /// </summary>
    /// <param name="aggs"></param>
    /// <returns></returns>
    public DataFrame Agg(params Expr[] aggs)
    {
        var byHandles = _by.Select(b => PolarsWrapper.CloneExpr(b.Handle)).ToArray();
        var aggHandles = aggs.Select(a => PolarsWrapper.CloneExpr(a.Handle)).ToArray();

        var h = PolarsWrapper.GroupByAgg(_df.Handle, byHandles, aggHandles);
        return new DataFrame(h);
    }
}

/// <summary>
/// A helper class to construct a dynamic groupby operation on a DataFrame.
/// </summary>
public class DynamicGroupBy
{
    private readonly DataFrame _df;
    private readonly string _indexColumn;
    private readonly TimeSpan _every;
    private readonly TimeSpan? _period;
    private readonly TimeSpan? _offset;
    private readonly Expr[]? _by;
    private readonly Label _label;
    private readonly bool _includeBoundaries;
    private readonly ClosedWindow _closedWindow;
    private readonly StartBy _startBy;

    internal DynamicGroupBy(
        DataFrame df,
        string indexColumn,
        TimeSpan every,
        TimeSpan? period,
        TimeSpan? offset,
        Expr[]? by,
        Label label,
        bool includeBoundaries,
        ClosedWindow closedWindow,
        StartBy startBy)
    {
        _df = df;
        _indexColumn = indexColumn;
        _every = every;
        _period = period;
        _offset = offset;
        _by = by;
        _label = label;
        _includeBoundaries = includeBoundaries;
        _closedWindow = closedWindow;
        _startBy = startBy;
    }

    /// <summary>
    /// Apply aggregations to the dynamic group.
    /// </summary>
    public DataFrame Agg(params Expr[] aggs)
    {
        // 核心逻辑：Eager -> Lazy -> GroupByDynamic -> Agg -> Collect -> Eager
        // 这一步非常高效，因为 Lazy 引擎会优化整个聚合计划
        return _df.Lazy()
            .GroupByDynamic(
                _indexColumn,
                _every,
                _period,
                _offset,
                _by,
                _label,
                _includeBoundaries,
                _closedWindow,
                _startBy
            )
            .Agg(aggs)
            .Collect();
    }
}