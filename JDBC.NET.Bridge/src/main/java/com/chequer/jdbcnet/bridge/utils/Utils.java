package com.chequer.jdbcnet.bridge.utils;

import proto.Common;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Optional;

public class Utils {
    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    public static ByteBuffer intToBytes(int value){
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).flip();
    }

    public static String bytesToHex(byte[] bytes) {
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return "0x" + new String(hexChars, StandardCharsets.UTF_8);
    }

    public static void addColumns(Common.JdbcResultSetResponse.Builder builder, ResultSetMetaData metaData) throws SQLException {
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            var columnName = metaData.getColumnName(i);
            var columnLabel = metaData.getColumnLabel(i);

            builder.addColumns(Common.JdbcDataColumn.newBuilder()
                    .setOrdinal(i - 1)
                    .setTableName(Optional.ofNullable(metaData.getTableName(i)).orElse(""))
                    .setSchemaName(Optional.ofNullable(metaData.getSchemaName(i)).orElse(""))
                    .setCatalogName(Optional.ofNullable(metaData.getCatalogName(i)).orElse(""))
                    .setColumnName(Optional.ofNullable(columnName).orElse(""))
                    .setColumnLabel(Optional.ofNullable(columnLabel).orElse(""))
                    .setColumnDisplaySize(metaData.getColumnDisplaySize(i))
                    .setColumnPrecision(metaData.getPrecision(i))
                    .setColumnScale(metaData.getScale(i))
                    .setDataTypeName(Optional.ofNullable(metaData.getColumnTypeName(i)).orElse(""))
                    .setDataTypeClassName(Optional.ofNullable(metaData.getColumnClassName(i)).orElse(""))
                    .setDataTypeCode(metaData.getColumnType(i))
                    .setIsAutoIncrement(metaData.isAutoIncrement(i))
                    .setIsCaseSensitive(metaData.isCaseSensitive(i))
                    .setIsDefinitelyWritable(metaData.isDefinitelyWritable(i))
                    .setIsSearchable(metaData.isSearchable(i))
                    .setIsNullable(metaData.isNullable(i))
                    .setIsAliased(!columnName.equals(columnLabel))
                    .setIsWritable(metaData.isWritable(i))
                    .setIsCurrency(metaData.isCurrency(i))
                    .setIsReadOnly(metaData.isReadOnly(i))
                    .setIsSigned(metaData.isSigned(i))
                    .build());
        }
    }

    public static <T> T[] emptyArrayToNull(T[] array) {
        if (array.length <= 0)
            return null;

        return array;
    }
}
