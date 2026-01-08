using System.Collections;
using Apache.Arrow;

namespace Polars.NET.Core.Arrow
{
    /// <summary>
    /// Combining Enumerator: Combine Pre-Read Head (First Batch) with Tail into full stream.
    /// </summary>
    public class PrependEnumerator: IEnumerator<RecordBatch>
    {
        private bool _isFirst = true;
        private readonly RecordBatch _head;
        private readonly IEnumerator<RecordBatch> _tail;

        public PrependEnumerator(RecordBatch head, IEnumerator<RecordBatch> tail)
        {
            _head = head;
            _tail = tail;
        }

        public RecordBatch Current => _isFirst ? _head : _tail.Current;
        object IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (_isFirst)
            {
                _isFirst = false;
                return true;
            }
            return _tail.MoveNext();
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose()
        {
            _head.Dispose();
            _tail.Dispose();
        }
    }
}