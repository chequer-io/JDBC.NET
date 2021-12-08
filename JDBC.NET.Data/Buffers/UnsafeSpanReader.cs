using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;

namespace JDBC.NET.Data.Utilities
{
    public ref struct UnsafeSpanReader
    {
        public int Position
        {
            get => _position;
            set
            {
                if (value > _input.Length)
                    throw new EndOfStreamException();

                _position = value;
            }
        }

        public int Remaining => _input.Length - _position;

        public int Length => _input.Length;

        private readonly ReadOnlySpan<byte> _input;
        private int _position;

        public UnsafeSpanReader(ReadOnlySpan<byte> input)
        {
            _input = input;
            _position = 0;
        }

        public byte PeekByte()
        {
            if (_position >= _input.Length)
                throw new EndOfStreamException();

            return _input[_position];
        }

        public byte ReadByte()
        {
            if (_position >= _input.Length)
                throw new EndOfStreamException();

            return _input[_position++];
        }

        public byte ReadByte(byte expected)
        {
            if (_position >= _input.Length)
                throw new EndOfStreamException();

            var value = _input[_position++];

            if (value != expected)
                throw new FormatException($"Expected to read 0x{value:X2} but got 0x{expected:X2}");

            return value;
        }

        #region Little Endian
        public short ReadInt16()
        {
            if (_position + 2 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 2);
            _position += 2;

            return BinaryPrimitives.ReadInt16LittleEndian(span);
        }

        public ushort ReadUInt16()
        {
            if (_position + 2 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 2);
            _position += 2;

            return BinaryPrimitives.ReadUInt16LittleEndian(span);
        }

        public int ReadInt32()
        {
            if (_position + 4 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 4);
            _position += 4;

            return BinaryPrimitives.ReadInt32LittleEndian(span);
        }

        public uint ReadUInt32()
        {
            if (_position + 4 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 4);
            _position += 4;

            return BinaryPrimitives.ReadUInt32LittleEndian(span);
        }

        public long ReadInt64()
        {
            if (_position + 8 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 8);
            _position += 8;
            return BinaryPrimitives.ReadInt64LittleEndian(span);
        }

        public ulong ReadUInt64()
        {
            if (_position + 8 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 8);
            _position += 8;

            return BinaryPrimitives.ReadUInt64LittleEndian(span);
        }

        public float ReadSingle()
        {
            if (_position + 4 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 4);
            _position += 4;

            return BinaryPrimitives.ReadSingleLittleEndian(span);
        }

        public double ReadDouble()
        {
            if (_position + 8 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 8);
            _position += 8;

            return BinaryPrimitives.ReadDoubleLittleEndian(span);
        }
        #endregion

        #region Little Endian (length)
        public short ReadInt16(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadInt16LittleEndian(span, length);
        }

        public ushort ReadUInt16(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadUInt16LittleEndian(span, length);
        }

        public int ReadInt32(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadInt32LittleEndian(span, length);
        }

        public uint ReadUInt32(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadUInt32LittleEndian(span, length);
        }

        public long ReadInt64(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadInt64LittleEndian(span, length);
        }

        public ulong ReadUInt64(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadUInt64LittleEndian(span, length);
        }

        public float ReadSingle(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadSingleLittleEndian(span, length);
        }

        public double ReadDouble(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadDoubleLittleEndian(span, length);
        }
        #endregion

        #region Big Endian
        public short ReadInt16BigEndian()
        {
            if (_position + 2 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 2);
            _position += 2;

            return BinaryPrimitives.ReadInt16BigEndian(span);
        }

        public ushort ReadUInt16BigEndian()
        {
            if (_position + 2 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 2);
            _position += 2;

            return BinaryPrimitives.ReadUInt16BigEndian(span);
        }

        public int ReadInt32BigEndian()
        {
            if (_position + 4 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 4);
            _position += 4;

            return BinaryPrimitives.ReadInt32BigEndian(span);
        }

        public uint ReadUInt32BigEndian()
        {
            if (_position + 4 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 4);
            _position += 4;

            return BinaryPrimitives.ReadUInt32BigEndian(span);
        }

        public long ReadInt64BigEndian()
        {
            if (_position + 8 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 8);
            _position += 8;

            return BinaryPrimitives.ReadInt64BigEndian(span);
        }

        public ulong ReadUInt64BigEndian()
        {
            if (_position + 8 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 8);
            _position += 8;

            return BinaryPrimitives.ReadUInt64BigEndian(span);
        }

        public float ReadSingleBigEndian()
        {
            if (_position + 4 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 4);
            _position += 4;

            return BinaryPrimitives.ReadSingleBigEndian(span);
        }

        public double ReadDoubleBigEndian()
        {
            if (_position + 8 > _input.Length)
                throw new IndexOutOfRangeException();

            ReadOnlySpan<byte> span = _input.Slice(_position, 8);
            _position += 8;

            return BinaryPrimitives.ReadDoubleBigEndian(span);
        }
        #endregion

        #region Big Endian (length)
        public short ReadInt16BigEndian(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadInt16BigEndian(span, length);
        }

        public ushort ReadUInt16BigEndian(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadUInt16BigEndian(span, length);
        }

        public int ReadInt32BigEndian(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadInt32BigEndian(span, length);
        }

        public uint ReadUInt32BigEndian(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadUInt32BigEndian(span, length);
        }

        public long ReadInt64BigEndian(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadInt64BigEndian(span, length);
        }

        public ulong ReadUInt64BigEndian(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadUInt64BigEndian(span, length);
        }

        public float ReadSingleBigEndian(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadSingleBigEndian(span, length);
        }

        public double ReadDoubleBigEndian(int length)
        {
            if (length == 0)
                return 0;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            _position += length;

            return UnsafeBinaryPrimitives.ReadDoubleBigEndian(span, length);
        }
        #endregion

        public byte[] ReadBytes(int count)
        {
            if (count == 0)
                return Array.Empty<byte>();

            return ReadSpan(count).ToArray();
        }

        public ReadOnlySpan<byte> ReadSpan(int count)
        {
            if (_position + count > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(count));

            ReadOnlySpan<byte> result = _input.Slice(_position, count);
            _position += count;

            return result;
        }

        public string ReadString(int length, Encoding encoding = null)
        {
            if (length == 0)
                return string.Empty;

            if (_position + length > _input.Length)
                throw new ArgumentOutOfRangeException(nameof(length));

            ReadOnlySpan<byte> span = _input.Slice(_position, length);
            var result = (encoding ?? Encoding.UTF8).GetString(span);
            _position += length;

            return result;
        }

        public string ReadNullTerminatedString(Encoding encoding = null)
        {
            var index = _input.Slice(_position).IndexOf((byte)0);

            if (index == -1)
                index = _input.Length - _position;

            var result = (encoding ?? Encoding.UTF8).GetString(_input.Slice(_position, index));
            _position += index + 1;

            return result;
        }

        public byte[] ReadNullTerminatedStringBytes()
        {
            var index = _input.Slice(_position).IndexOf((byte)0);

            if (index == -1)
                index = _input.Length - _position;

            var result = _input.Slice(_position, index).ToArray();
            _position += index + 1;

            return result;
        }
    }
}
