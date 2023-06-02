using System;
using System.Numerics;
using System.Text;
using JDBC.NET.Proto;

namespace JDBC.NET.Data;

#if NET6_0_OR_GREATER
using BinaryPrimitives = System.Buffers.Binary.BinaryPrimitives;
#else
using BinaryPrimitives = JDBC.NET.Data.Utilities.BinaryPrimitivesCompat;
#endif

internal partial class JdbcDataChunk
{
    private delegate object Decoder(Type fieldType, in ReadOnlySpan<byte> data, ref int position);

    private static readonly Decoder[] _decoders =
    {
        DecodeNull,
        DecodeText,
        DecodeByte,
        DecodeShort,
        DecodeInteger,
        DecodeLong,
        DecodeFloat,
        DecodeDouble,
        DecodeChar,
        DecodeBoolean,
        DecodeBigInteger,
        DecodeBigDecimal,
        DecodeDate,
        DecodeTime,
        DecodeDateTime,
        DecodeBinary,
        DecodeUnknown
    };

    private static object Decode(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var type = data[position++];

        if (type is < (byte)JdbcItemType.Null or > (byte)JdbcItemType.Unknown)
            throw new NotSupportedException($"{(JdbcItemType)type} Decoder");

        return _decoders[type](fieldType, data, ref position);
    }

    private static object DecodeNull(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        return DBNull.Value;
    }

    private static object DecodeText(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var length = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(position, 4));
        var value = Encoding.UTF8.GetString(data.Slice(position + 4, length));
        position += 4 + length;
        return value;
    }

    private static object DecodeByte(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        return data[position++];
    }

    private static object DecodeShort(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var value = BinaryPrimitives.ReadInt16LittleEndian(data.Slice(position, 2));
        position += 2;
        return value;
    }

    private static object DecodeInteger(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var value = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(position, 4));
        position += 4;
        return value;
    }

    private static object DecodeLong(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var value = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(position, 8));
        position += 8;
        return value;
    }

    private static object DecodeFloat(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var value = BinaryPrimitives.ReadSingleLittleEndian(data.Slice(position, 4));
        position += 4;
        return value;
    }

    private static object DecodeDouble(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var value = BinaryPrimitives.ReadDoubleLittleEndian(data.Slice(position, 8));
        position += 8;
        return value;
    }

    private static object DecodeChar(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var value = (char)BinaryPrimitives.ReadInt16LittleEndian(data.Slice(position, 2));
        position += 2;
        return value;
    }

    private static object DecodeBoolean(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        return data[position++] is not 0;
    }

    private static object DecodeBigInteger(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var length = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(position, 4));
        var bytes = data.Slice(position + 4, length).ToArray();
        bytes.AsSpan().Reverse();
        var value = new BigInteger(bytes);
        position += 4 + length;
        return value;
    }

    private static object DecodeBigDecimal(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var scale = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(position, 4));
        var length = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(position + 4, 4));
        var bytes = data.Slice(position + 8, length).ToArray();
        bytes.AsSpan().Reverse();
        var value = new BigInteger(bytes).ToString($"D{scale + 1}");
        position += 8 + length;

        return string.Create(
            value.Length + 1,
            (value, scale),
            static (buffer, args) =>
            {
                var (bigIntStr, scale) = args;

                bigIntStr.AsSpan()[..^scale].CopyTo(buffer);

                var offset = bigIntStr.Length - scale;

                buffer[offset++] = '.';
                bigIntStr.AsSpan()[^scale..].CopyTo(buffer[offset..]);
            });
    }

    private static object DecodeDate(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var time = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(position, 8));
        position += 8;
        var date = DateTime.UnixEpoch.AddMilliseconds(time).ToLocalTime();
#if NET6_0_OR_GREATER
        return DateOnly.FromDateTime(date);
#else
        return date;
#endif
    }

    private static object DecodeTime(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var time = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(position, 8));
        position += 8;
        return DateTime.UnixEpoch.AddMilliseconds(time).ToLocalTime().TimeOfDay;
    }

    private static object DecodeDateTime(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var time = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(position, 8));
        position += 8;
        return DateTime.UnixEpoch.AddMilliseconds(time).ToLocalTime();
    }

    private static object DecodeBinary(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var length = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(position, 4));
        var value = data.Slice(position + 4, length).ToArray();
        position += 4 + length;
        return value;
    }

    private static object DecodeUnknown(Type fieldType, in ReadOnlySpan<byte> data, ref int position)
    {
        var length = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(position, 4));
        var value = Encoding.UTF8.GetString(data.Slice(position + 4, length));
        position += 4 + length;

        return fieldType != typeof(DateTimeOffset)
            ? Convert.ChangeType(value, fieldType)
            : DateTimeOffset.Parse(value);
    }
}
