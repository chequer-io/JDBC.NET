using System;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace JDBC.NET.Data.Utilities;

// https://github.com/dotnet/runtime/blob/main/src/libraries/System.Private.CoreLib/src/System/Buffers/Binary/BinaryPrimitives.ReadLittleEndian.cs
internal static class BinaryPrimitivesCompat
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object ReadDoubleLittleEndian(ReadOnlySpan<byte> source)
    {
        return !BitConverter.IsLittleEndian ?
            BitConverter.Int64BitsToDouble(BinaryPrimitives.ReverseEndianness(MemoryMarshal.Read<long>(source))) :
            MemoryMarshal.Read<double>(source);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float ReadSingleLittleEndian(ReadOnlySpan<byte> source)
    {
        return !BitConverter.IsLittleEndian ?
            BitConverter.Int32BitsToSingle(BinaryPrimitives.ReverseEndianness(MemoryMarshal.Read<int>(source))) :
            MemoryMarshal.Read<float>(source);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ReadInt32LittleEndian(ReadOnlySpan<byte> source)
    {
        return BinaryPrimitives.ReadInt32LittleEndian(source);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static short ReadInt16LittleEndian(ReadOnlySpan<byte> source)
    {
        return BinaryPrimitives.ReadInt16LittleEndian(source);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ReadInt64LittleEndian(ReadOnlySpan<byte> source)
    {
        return BinaryPrimitives.ReadInt64LittleEndian(source);
    }
}
