using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace JDBC.NET.Data.Utilities
{
    public static unsafe class UnsafeBinaryPrimitives
    {
        #region Read - Little Endian
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static short ReadInt16LittleEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<short>())
                return BinaryPrimitives.ReadInt16LittleEndian(span);

            return Read<short>(span, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort ReadUInt16LittleEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<ushort>())
                return BinaryPrimitives.ReadUInt16LittleEndian(span);

            return Read<ushort>(span, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadInt32LittleEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<int>())
                return BinaryPrimitives.ReadInt32LittleEndian(span);

            return Read<int>(span, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint ReadUInt32LittleEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<uint>())
                return BinaryPrimitives.ReadUInt32LittleEndian(span);

            return Read<uint>(span, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ReadInt64LittleEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<long>())
                return BinaryPrimitives.ReadInt64LittleEndian(span);

            return Read<long>(span, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong ReadUInt64LittleEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<ulong>())
                return BinaryPrimitives.ReadUInt64LittleEndian(span);

            return Read<ulong>(span, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float ReadSingleLittleEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<float>())
                return BinaryPrimitives.ReadSingleLittleEndian(span);

            return Read<float>(span, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static double ReadDoubleLittleEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<double>())
                return BinaryPrimitives.ReadDoubleLittleEndian(span);

            return Read<double>(span, length);
        }
        #endregion

        #region Read - Big Endian
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static short ReadInt16BigEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<short>())
                return BinaryPrimitives.ReadInt16BigEndian(span);

            return ReverseEndianness(Read<short>(span, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort ReadUInt16BigEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<ushort>())
                return BinaryPrimitives.ReadUInt16BigEndian(span);

            return ReverseEndianness(Read<ushort>(span, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadInt32BigEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<int>())
                return BinaryPrimitives.ReadInt32BigEndian(span);

            return ReverseEndianness(Read<int>(span, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint ReadUInt32BigEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<uint>())
                return BinaryPrimitives.ReadUInt32BigEndian(span);

            return ReverseEndianness(Read<uint>(span, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ReadInt64BigEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<long>())
                return BinaryPrimitives.ReadInt64BigEndian(span);

            return ReverseEndianness(Read<long>(span, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong ReadUInt64BigEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<ulong>())
                return BinaryPrimitives.ReadUInt64BigEndian(span);

            return ReverseEndianness(Read<ulong>(span, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float ReadSingleBigEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<float>())
                return BinaryPrimitives.ReadSingleBigEndian(span);

            return BitConverter.Int32BitsToSingle(ReverseEndianness(Read<int>(span, length), length));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static double ReadDoubleBigEndian(ReadOnlySpan<byte> span, int length)
        {
            if (length == Unsafe.SizeOf<double>())
                return BinaryPrimitives.ReadDoubleBigEndian(span);

            return BitConverter.Int64BitsToDouble(ReverseEndianness(Read<long>(span, length), length));
        }
        #endregion

        #region Write - Little Endian
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteInt16LittleEndian(Span<byte> span, short value, int length)
        {
            if (length == Unsafe.SizeOf<short>())
            {
                BinaryPrimitives.WriteInt16LittleEndian(span, value);
                return;
            }

            Write(span, value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteUInt16LittleEndian(Span<byte> span, ushort value, int length)
        {
            if (length == Unsafe.SizeOf<ushort>())
            {
                BinaryPrimitives.WriteUInt16LittleEndian(span, value);
                return;
            }

            Write(span, value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteInt32LittleEndian(Span<byte> span, int value, int length)
        {
            if (length == Unsafe.SizeOf<int>())
            {
                BinaryPrimitives.WriteInt32LittleEndian(span, value);
                return;
            }

            Write(span, value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteUInt32LittleEndian(Span<byte> span, uint value, int length)
        {
            if (length == Unsafe.SizeOf<uint>())
            {
                BinaryPrimitives.WriteUInt32LittleEndian(span, value);
                return;
            }

            Write(span, value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteInt64LittleEndian(Span<byte> span, long value, int length)
        {
            if (length == Unsafe.SizeOf<long>())
            {
                BinaryPrimitives.WriteInt64LittleEndian(span, value);
                return;
            }

            Write(span, value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteUInt64LittleEndian(Span<byte> span, ulong value, int length)
        {
            if (length == Unsafe.SizeOf<ulong>())
            {
                BinaryPrimitives.WriteUInt64LittleEndian(span, value);
                return;
            }

            Write(span, value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteSingleLittleEndian(Span<byte> span, float value, int length)
        {
            if (length == Unsafe.SizeOf<float>())
            {
                BinaryPrimitives.WriteSingleLittleEndian(span, value);
                return;
            }

            Write(span, value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteDoubleLittleEndian(Span<byte> span, double value, int length)
        {
            if (length == Unsafe.SizeOf<double>())
            {
                BinaryPrimitives.WriteDoubleLittleEndian(span, value);
                return;
            }

            Write(span, value, length);
        }
        #endregion

        #region Write - Big Endian
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteInt16BigEndian(Span<byte> span, short value, int length)
        {
            if (length == Unsafe.SizeOf<short>())
            {
                BinaryPrimitives.WriteInt16BigEndian(span, value);
                return;
            }

            Write(span, ReverseEndianness(value, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteUInt16BigEndian(Span<byte> span, ushort value, int length)
        {
            if (length == Unsafe.SizeOf<ushort>())
            {
                BinaryPrimitives.WriteUInt16BigEndian(span, value);
                return;
            }

            Write(span, ReverseEndianness(value, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteInt32BigEndian(Span<byte> span, int value, int length)
        {
            if (length == Unsafe.SizeOf<int>())
            {
                BinaryPrimitives.WriteInt32BigEndian(span, value);
                return;
            }

            Write(span, ReverseEndianness(value, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteUInt32BigEndian(Span<byte> span, uint value, int length)
        {
            if (length == Unsafe.SizeOf<uint>())
            {
                BinaryPrimitives.WriteUInt32BigEndian(span, value);
                return;
            }

            Write(span, ReverseEndianness(value, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteInt64BigEndian(Span<byte> span, long value, int length)
        {
            if (length == Unsafe.SizeOf<long>())
            {
                BinaryPrimitives.WriteInt64BigEndian(span, value);
                return;
            }

            Write(span, ReverseEndianness(value, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteUInt64BigEndian(Span<byte> span, ulong value, int length)
        {
            if (length == Unsafe.SizeOf<ulong>())
            {
                BinaryPrimitives.WriteUInt64BigEndian(span, value);
                return;
            }

            Write(span, ReverseEndianness(value, length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteSingleBigEndian(Span<byte> span, float value, int length)
        {
            if (length == Unsafe.SizeOf<float>())
            {
                BinaryPrimitives.WriteSingleBigEndian(span, value);
                return;
            }

            Write(span, ReverseEndianness(BitConverter.SingleToInt32Bits(value), length), length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void WriteDoubleBigEndian(Span<byte> span, double value, int length)
        {
            if (length == Unsafe.SizeOf<double>())
            {
                BinaryPrimitives.WriteDoubleBigEndian(span, value);
                return;
            }

            Write(span, ReverseEndianness(BitConverter.DoubleToInt64Bits(value), length), length);
        }
        #endregion

        #region ReverseEndianness
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static short ReverseEndianness(short value, int length)
        {
            return (short)ReverseEndianness((ushort)value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ushort ReverseEndianness(ushort value, int length)
        {
            switch (length)
            {
                case 1:
                    return (ushort)(value & 0x00_FF);

                case 2:
                    return BinaryPrimitives.ReverseEndianness(value);

                default:
                    throw new ArgumentOutOfRangeException(nameof(length));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReverseEndianness(int value, int length)
        {
            return (int)ReverseEndianness((uint)value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static uint ReverseEndianness(uint value, int length)
        {
            switch (length)
            {
                case 1:
                    return (byte)value;

                case 2:
                    // Input: value = [ ww xx yy zz ]
                    //
                    // First line generators : [ ww xx | yy zz ]
                    //                       & [ 00 00 | 00 FF ]
                    //                       = [ 00 00 | 00 zz ]
                    //                 SL(8) = [ 00 00 | zz 00 ]
                    //
                    // Second line generates : [ ww xx | yy zz ]
                    //                       & [ 00 00 | FF 00 ]
                    //                       = [ 00 00 | yy 00 ]
                    //                 SR(8) = [ 00 00 | 00 yy ]
                    //
                    //                 (sum) = [ 00 00 | zz yy ]
                    //
                    return
                        ((value & 0x00_00_00_FFu) << 8) +
                        ((value & 0x00_00_FF_00u) >> 8);

                case 3:
                    // Input: value = [ ww xx yy zz ]
                    //
                    // First line generators : [ ww | xx yy zz ]
                    //                       & [ 00 | 00 00 FF ]
                    //                       = [ 00 | 00 00 zz ]
                    //                SL(16) = [ 00 | zz 00 00 ]
                    //
                    // Second line generates : [ ww | xx yy zz ]
                    //                       & [ 00 | FF 00 00 ]
                    //                       = [ 00 | xx 00 00 ]
                    //                SR(16) = [ 00 | 00 00 xx ]
                    //
                    // Third line generates  : [ ww | xx yy zz ]
                    //                       & [ 00 | 00 FF 00 ]
                    //                       = [ 00 | 00 yy 00 ]
                    //
                    //                 (sum) = [ 00 | zz yy xx ]
                    //
                    return
                        ((value & 0x00_00_00_FFu) << 16) +
                        ((value & 0x00_FF_00_00u) >> 16) +
                        (value & 0x00_00_FF_00u);

                case 4:
                    return BinaryPrimitives.ReverseEndianness(value);

                default:
                    throw new ArgumentOutOfRangeException(nameof(length));
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long ReverseEndianness(long value, int length)
        {
            return (long)ReverseEndianness((ulong)value, length);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ulong ReverseEndianness(ulong value, int length)
        {
            switch (length)
            {
                case <= 4:
                    return ReverseEndianness((uint)value, length);

                case 5:
                    return BinaryPrimitives.ReverseEndianness(value) >> 24;

                case 6:
                    return BinaryPrimitives.ReverseEndianness(value) >> 16;

                case 7:
                    return BinaryPrimitives.ReverseEndianness(value) >> 8;

                case 8:
                    return BinaryPrimitives.ReverseEndianness(value);

                default:
                    throw new ArgumentOutOfRangeException(nameof(length));
            }
        }
        #endregion

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static T Read<T>(ReadOnlySpan<byte> span, int length)
        {
            if (span.Length < length || Unsafe.SizeOf<T>() < length)
                throw new ArgumentOutOfRangeException(nameof(length));

            T result = default;

            Unsafe.CopyBlock(
                Unsafe.AsPointer(ref result),
                Unsafe.AsPointer(ref MemoryMarshal.GetReference(span)),
                (uint)length);

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void Write<T>(Span<byte> span, T value, int length)
        {
            if (span.Length < length || Unsafe.SizeOf<T>() < length)
                throw new ArgumentOutOfRangeException(nameof(length));

            Unsafe.CopyBlock(
                Unsafe.AsPointer(ref MemoryMarshal.GetReference(span)),
                Unsafe.AsPointer(ref value),
                (uint)length);
        }
    }
}
