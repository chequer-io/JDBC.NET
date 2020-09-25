using System;
using System.Collections.Generic;
using JDBC.NET.Data.Models;

namespace JDBC.NET.Data.Converters
{
    public static class JdbcTypeConverter
    {
        #region Fields
        private static readonly Dictionary<JdbcDataTypeCode, Type> _dictionary = new Dictionary<JdbcDataTypeCode, Type>
        {
            { JdbcDataTypeCode.ARRAY, typeof(Array) },
            { JdbcDataTypeCode.BIGINT, typeof(long) },
            { JdbcDataTypeCode.BINARY, typeof(byte[]) },
            { JdbcDataTypeCode.BIT, typeof(bool) },
            { JdbcDataTypeCode.BLOB, typeof(byte[]) },
            { JdbcDataTypeCode.BOOLEAN, typeof(bool) },
            { JdbcDataTypeCode.CHAR, typeof(string) },
            { JdbcDataTypeCode.CLOB, typeof(string) },
            { JdbcDataTypeCode.DATALINK, typeof(string) },
            { JdbcDataTypeCode.DATE, typeof(DateTime) },
            { JdbcDataTypeCode.DECIMAL, typeof(decimal) },
            { JdbcDataTypeCode.DISTINCT, typeof(object) },
            { JdbcDataTypeCode.DOUBLE, typeof(double) },
            { JdbcDataTypeCode.FLOAT, typeof(float) },
            { JdbcDataTypeCode.INTEGER, typeof(int) },
            { JdbcDataTypeCode.JAVA_OBJECT, typeof(object) },
            { JdbcDataTypeCode.LONGNVARCHAR, typeof(string) },
            { JdbcDataTypeCode.LONGVARBINARY, typeof(byte[]) },
            { JdbcDataTypeCode.LONGVARCHAR, typeof(string) },
            { JdbcDataTypeCode.NCHAR, typeof(string) },
            { JdbcDataTypeCode.NCLOB, typeof(string) },
            { JdbcDataTypeCode.NULL, typeof(DBNull) },
            { JdbcDataTypeCode.NUMERIC, typeof(decimal) },
            { JdbcDataTypeCode.NVARCHAR, typeof(string) },
            { JdbcDataTypeCode.OTHER, typeof(object) },
            { JdbcDataTypeCode.REAL, typeof(float) },
            { JdbcDataTypeCode.REF, typeof(IntPtr) },
            { JdbcDataTypeCode.REF_CURSOR, typeof(IntPtr) },
            { JdbcDataTypeCode.ROWID, typeof(int) },
            { JdbcDataTypeCode.SMALLINT, typeof(short) },
            { JdbcDataTypeCode.SQLXML, typeof(string) },
            { JdbcDataTypeCode.STRUCT, typeof(object) },
            { JdbcDataTypeCode.TIME, typeof(TimeSpan) },
            { JdbcDataTypeCode.TIME_WITH_TIMEZONE, typeof(DateTimeOffset) },
            { JdbcDataTypeCode.TIMESTAMP, typeof(DateTime) },
            { JdbcDataTypeCode.TIMESTAMP_WITH_TIMEZONE, typeof(DateTimeOffset) },
            { JdbcDataTypeCode.TINYINT, typeof(short) },
            { JdbcDataTypeCode.VARBINARY, typeof(byte[]) },
            { JdbcDataTypeCode.VARCHAR, typeof(string) },
        };
        #endregion

        #region Public Methods
        public static Type ToType(JdbcDataTypeCode typeCode)
        {
            return !_dictionary.TryGetValue(typeCode, out var type)
                ? typeof(object) 
                : type;
        }
        #endregion
    }
}
