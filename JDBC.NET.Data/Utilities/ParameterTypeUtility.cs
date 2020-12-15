using System;
using System.Collections.Generic;
using System.Data;
using JDBC.NET.Proto;

namespace JDBC.NET.Data.Utilities
{
    public static class ParameterTypeUtility
    {
        #region Fields
        private static readonly Dictionary<Type, DbType> _typeMap = new()
        {
            { typeof(byte), DbType.Byte },
            { typeof(sbyte), DbType.SByte },
            { typeof(short), DbType.Int16 },
            { typeof(ushort), DbType.UInt16 },
            { typeof(int), DbType.Int32 },
            { typeof(uint), DbType.UInt32 },
            { typeof(long), DbType.Int64 },
            { typeof(ulong), DbType.UInt64 },
            { typeof(float), DbType.Single },
            { typeof(double), DbType.Double },
            { typeof(decimal), DbType.Decimal },
            { typeof(bool), DbType.Boolean },
            { typeof(string), DbType.String },
            { typeof(char), DbType.StringFixedLength },
            { typeof(Guid), DbType.Guid },
            { typeof(DateTime), DbType.DateTime },
            { typeof(DateTimeOffset), DbType.DateTimeOffset },
            { typeof(byte[]), DbType.Binary },
            { typeof(byte?), DbType.Byte },
            { typeof(sbyte?), DbType.SByte },
            { typeof(short?), DbType.Int16 },
            { typeof(ushort?), DbType.UInt16 },
            { typeof(int?), DbType.Int32 },
            { typeof(uint?), DbType.UInt32 },
            { typeof(long?), DbType.Int64 },
            { typeof(ulong?), DbType.UInt64 },
            { typeof(float?), DbType.Single },
            { typeof(double?), DbType.Double },
            { typeof(decimal?), DbType.Decimal },
            { typeof(bool?), DbType.Boolean },
            { typeof(char?), DbType.StringFixedLength },
            { typeof(Guid?), DbType.Guid },
            { typeof(DateTime?), DbType.DateTime },
            { typeof(DateTimeOffset?), DbType.DateTimeOffset }
        };
        #endregion

        public static ParameterType Convert(DbType type)
        {
            return type switch
            {
                DbType.Date => ParameterType.Date,
                DbType.Time => ParameterType.Time,
                DbType.Int16 => ParameterType.Short,
                DbType.Int32 => ParameterType.Int,
                DbType.Int64 => ParameterType.Long,
                DbType.Single => ParameterType.Float,
                DbType.Double => ParameterType.Double,
                DbType.String => ParameterType.String,
                DbType.Boolean => ParameterType.Boolean,
                _ => throw new NotSupportedException($"{type} is not yet supported.")
            };
        }

        public static DbType Convert(object value)
        {
            var valueType = value.GetType();

            if (!_typeMap.TryGetValue(valueType, out var type))
                throw new NotSupportedException($"{valueType} is not yet supported.");

            return type;
        }
    }
}
