package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.chequer.jdbcnet.bridge.utils.Utils;
import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.netty.shaded.io.netty.buffer.ByteBuf;
import io.grpc.netty.shaded.io.netty.buffer.ByteBufAllocator;
import io.grpc.netty.shaded.io.netty.util.ReferenceCountUtil;
import io.grpc.stub.StreamObserver;
import proto.Common;
import proto.reader.Reader;
import proto.reader.ReaderServiceGrpc;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class ReaderServiceImpl extends ReaderServiceGrpc.ReaderServiceImplBase {
    @Override
    public StreamObserver<Reader.ReadResultSetRequest> readResultSet(final StreamObserver<Reader.ReadResultSetResponse> responseObserver) {
        return new StreamObserver<>() {
            public void onNext(Reader.ReadResultSetRequest readResultSetRequest) {
                try {
                    var resultSet = ObjectManager.getResultSet(readResultSetRequest.getResultSetId());
                    var resultSetMetaData = resultSet.getMetaData();
                    var responseBuilder = Reader.ReadResultSetResponse.newBuilder();
                    var responseBuffer = ByteBufAllocator.DEFAULT.ioBuffer();
                    var columnCount = resultSetMetaData.getColumnCount();

                    try {
                        var start = System.currentTimeMillis();

                        while (resultSet.getHasRows()) {
                            for (int i = 1; i <= columnCount; i++) {
                                encodeValue(responseBuffer, resultSet.getObject(i));
                            }

                            if (System.currentTimeMillis() - start >= 1000 || responseBuffer.readableBytes() >= readResultSetRequest.getChunkSize()) {
                                responseBuilder.setRows(UnsafeByteOperations.unsafeWrap(responseBuffer.nioBuffer()));
                                responseBuilder.setIsCompleted(!resultSet.next());
                                responseObserver.onNext(responseBuilder.build());
                                return;
                            }

                            if (!resultSet.next()) {
                                break;
                            }
                        }

                        if (responseBuilder.getRows() != null) {
                            responseBuilder.setRows(UnsafeByteOperations.unsafeWrap(responseBuffer.nioBuffer()));
                            responseBuilder.setIsCompleted(true);
                            responseObserver.onNext(responseBuilder.build());
                        }
                    } finally {
                        responseBuffer.release();
                    }

                    onCompleted();
                } catch (Throwable e) {
                    onError(e);
                }
            }

            public void onError(Throwable throwable) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription(throwable.getMessage())
                        .asRuntimeException());
            }

            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    private void encodeValue(ByteBuf buffer, Object value) throws SQLException, IOException {
        if (value == null) {
            // | TYPE(1) |
            buffer.writeByte((byte) Common.JdbcItemType.NULL_VALUE);
        } else if (value instanceof String) {
            buffer.writeByte((byte) Common.JdbcItemType.TEXT_VALUE);
            writeText(buffer, (String) value);
        } else if (value instanceof Byte) {
            // | TYPE(1) | VALUE(1) |
            buffer.writeByte((byte) Common.JdbcItemType.BYTE_VALUE);
            buffer.writeByte((byte) value);
        } else if (value instanceof Short) {
            // | TYPE(1) | VALUE(2) |
            buffer.writeByte((byte) Common.JdbcItemType.SHORT_VALUE);
            buffer.writeShortLE((short) value);
        } else if (value instanceof Integer) {
            // | TYPE(1) | VALUE(4) |
            buffer.writeByte((byte) Common.JdbcItemType.INTEGER_VALUE);
            buffer.writeIntLE((int) value);
        } else if (value instanceof Long) {
            // | TYPE(1) | VALUE(8) |
            buffer.writeByte((byte) Common.JdbcItemType.LONG_VALUE);
            buffer.writeLongLE((long) value);
        } else if (value instanceof Float) {
            // | TYPE(1) | VALUE(4) |
            buffer.writeByte((byte) Common.JdbcItemType.FLOAT_VALUE);
            buffer.writeFloatLE((float) value);
        } else if (value instanceof Double) {
            // | TYPE(1) | VALUE(8) |
            buffer.writeByte((byte) Common.JdbcItemType.DOUBLE_VALUE);
            buffer.writeDoubleLE((double) value);
        } else if (value instanceof Character) {
            // | TYPE(1) | VALUE(2) |
            buffer.writeByte((byte) Common.JdbcItemType.CHAR_VALUE);
            buffer.writeShortLE((char) value);
        } else if (value instanceof Boolean) {
            // | TYPE(1) | VALUE(1) |
            buffer.writeByte((byte) Common.JdbcItemType.BOOLEAN_VALUE);
            buffer.writeBoolean((boolean) value);
        } else if (value instanceof BigInteger) {
            // | TYPE(1) | LENGTH(4) | VALUE(N) |
            var bigInteger = (BigInteger) value;
            buffer.writeByte((byte) Common.JdbcItemType.BIG_INTEGER_VALUE);
            buffer.writeIntLE(bigInteger.bitLength() / 8 + 1);
            buffer.writeBytes(bigInteger.toByteArray());
        } else if (value instanceof BigDecimal) {
            // | TYPE(1) | SCALE(4) | LENGTH(4) | VALUE(N) |
            var bigDecimal = (BigDecimal) value;
            var bigInteger = bigDecimal.unscaledValue();

            if (bigDecimal.scale() > 0) {
                buffer.writeByte((byte) Common.JdbcItemType.BIG_DECIMAL_VALUE);
                buffer.writeIntLE(bigDecimal.scale());
            } else {
                buffer.writeByte((byte) Common.JdbcItemType.BIG_INTEGER_VALUE);
            }

            buffer.writeIntLE(bigInteger.bitLength() / 8 + 1);
            buffer.writeBytes(bigInteger.toByteArray());
        } else if (value instanceof LocalDate) {
            // | TYPE(1) | VALUE(8) |
            buffer.writeByte((byte) Common.JdbcItemType.DATE_VALUE);
            buffer.writeLongLE(Date.valueOf((LocalDate) value).getTime());
        } else if (value instanceof LocalDateTime) {
            // | TYPE(1) | VALUE(8) |
            buffer.writeByte((byte) Common.JdbcItemType.DATE_TIME_VALUE);
            buffer.writeLongLE(Timestamp.valueOf((LocalDateTime) value).getTime());
        } else if (value instanceof LocalTime) {
            // | TYPE(1) | VALUE(8) |
            buffer.writeByte((byte) Common.JdbcItemType.TIME_VALUE);
            buffer.writeLongLE(java.sql.Time.valueOf((LocalTime) value).getTime());
        } else if (value.getClass() == java.sql.Date.class) {
            // | TYPE(1) | VALUE(8) |
            buffer.writeByte((byte) Common.JdbcItemType.DATE_VALUE);
            buffer.writeLongLE(((java.sql.Date) value).getTime());
        } else if (value.getClass() == java.sql.Time.class) {
            // | TYPE(1) | VALUE(8) |
            buffer.writeByte((byte) Common.JdbcItemType.TIME_VALUE);
            buffer.writeLongLE(((java.sql.Time) value).getTime());
        } else if (value instanceof java.util.Date) {
            // | TYPE(1) | VALUE(8) |
            buffer.writeByte((byte) Common.JdbcItemType.DATE_TIME_VALUE);
            buffer.writeLongLE(((java.util.Date) value).getTime());
        } else if (value instanceof byte[]) {
            // | TYPE(1) | LENGTH(4) | VALUE(N) |
            var byteValue = (byte[]) value;
            buffer.writeByte((byte) Common.JdbcItemType.BINARY_VALUE);
            buffer.writeIntLE(byteValue.length);
            buffer.writeBytes(byteValue);
        } else if (value instanceof Clob) {
            // | TYPE(1) | LENGTH(4) | VALUE(N) |
            try (var reader = ((Clob) value).getCharacterStream()) {
                buffer.writeByte((byte) Common.JdbcItemType.TEXT_VALUE);
                writeText(buffer, reader);
            }
        } else if (value instanceof Blob) {
            // | TYPE(1) | LENGTH(4) | VALUE(N) |
            try (var stream = ((Blob) value).getBinaryStream()) {
                buffer.writeByte((byte) Common.JdbcItemType.BINARY_VALUE);
                buffer.writeIntLE(0);
                var written = buffer.writeBytes(stream, -1);

                if (written > 0) {
                    buffer.markWriterIndex();
                    buffer.writerIndex(buffer.writerIndex() - written - 4);
                    buffer.writeIntLE(written);
                    buffer.resetWriterIndex();
                }
            }
        } else if (value instanceof Array) {
            var byteString = new StringBuilder("{");

            var arr = ((Object[]) ((Array) value).getArray());
            for (Object v : arr) {
                if (byteString.length() > 1) {
                    byteString.append(", ");
                }

                if (v instanceof byte[]) {
                    byteString.append(Utils.bytesToHex((byte[]) v));
                } else {
                    byteString.append(v);
                }
            }

            byteString.append("}");

            buffer.writeByte((byte) Common.JdbcItemType.TEXT_VALUE);
            writeText(buffer, byteString);
        } else {
            buffer.writeByte((byte) Common.JdbcItemType.UNKNOWN_VALUE);
            writeText(buffer, value.toString());
        }
    }

    private void writeText(ByteBuf buffer, CharSequence value) {
        buffer.writeIntLE(0);
        var written = buffer.writeCharSequence(value, StandardCharsets.UTF_8);

        if (written > 0) {
            buffer.markWriterIndex();
            buffer.writerIndex(buffer.writerIndex() - written - 4);
            buffer.writeIntLE(written);
            buffer.resetWriterIndex();
        }
    }

    private void writeText(ByteBuf buffer, java.io.Reader reader) throws IOException {
        buffer.writeIntLE(0);

        var written = 0;
        var charBuffer = CharBuffer.allocate(4096);
        int read;

        while ((read = reader.read(charBuffer)) >= 0) {
            charBuffer.flip();
            written += buffer.writeCharSequence(charBuffer.subSequence(0, read), StandardCharsets.UTF_8);
        }

        if (written > 0) {
            buffer.markWriterIndex();
            buffer.writerIndex(buffer.writerIndex() - written - 4);
            buffer.writeIntLE(written);
            buffer.resetWriterIndex();
        }
    }

    @Override
    public void closeResultSet(Reader.CloseResultSetRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var resultSet = ObjectManager.getResultSet(request.getResultSetId());
            resultSet.close();

            ObjectManager.removeResultSet(request.getResultSetId());

            var response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }
}
