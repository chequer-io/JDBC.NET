using System;
using System.Buffers.Binary;
using System.Text;
using JDBC.NET.Proto;

namespace JDBC.NET.Data;

internal sealed partial class JdbcDataChunk
{
    public object[] Current { get; }

    private ReadOnlyMemory<byte> _rows;
    private readonly Type[] _fieldTypes;

    public JdbcDataChunk(Type[] fieldTypes)
    {
        _fieldTypes = fieldTypes;
        Current = new object[fieldTypes.Length];
    }

    public void Update(ReadOnlyMemory<byte> rows)
    {
        _rows = rows;
    }

    public bool MoveNext()
    {
        if (_rows.IsEmpty)
            return false;

        ReadOnlySpan<byte> span = _rows.Span;
        var pos = 0;

        for (int i = 0; i < _fieldTypes.Length; i++)
            Current[i] = Decode(_fieldTypes[i], span, ref pos);

        _rows = _rows[pos..];

        return true;
    }
}
