package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.chequer.jdbcnet.bridge.utils.Utils;
import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.Common;
import proto.reader.Reader;
import proto.reader.ReaderServiceGrpc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;

public class ReaderServiceImpl extends ReaderServiceGrpc.ReaderServiceImplBase {
    @Override
    public StreamObserver<Reader.ReadResultSetRequest> readResultSet(final StreamObserver<Reader.ReadResultSetResponse> responseObserver) {
        return new StreamObserver<>() {
            public void onNext(Reader.ReadResultSetRequest readResultSetRequest) {
                try {
                    var resultSet = ObjectManager.getResultSet(readResultSetRequest.getResultSetId());
                    var resultSetMetaData = resultSet.getMetaData();
                    var responseBuilder = Reader.ReadResultSetResponse.newBuilder();

                    var inMemoryStream = new ByteArrayOutputStream();
                    var responseWriter = new DataOutputStream(inMemoryStream);

                    int readCount = 0;
                    do {
                        if (!resultSet.getHasRows())
                            break;

                        readCount++;

                        var columnCount = resultSetMetaData.getColumnCount();
                        responseWriter.write(Utils.intToBytes(columnCount));

                        for (int i = 1; i <= columnCount; i++) {
                            var value = resultSet.getObject(i);

                            if (value == null) {
                                responseWriter.write((byte)Common.JdbcItemType.NULL_VALUE);
                            } else if (value.getClass() == byte[].class) {
                                var byteValue = (byte[]) value;
                                responseWriter.write((byte)Common.JdbcItemType.BINARY_VALUE);
                                responseWriter.write(Utils.intToBytes(byteValue.length));
                                responseWriter.write(byteValue);
                            } else if (value instanceof Clob) {
                                var reader = ((Clob) value).getCharacterStream();

                                var builder = new StringBuilder();

                                while (true) {
                                    var data = reader.read();
                                    if (data == -1) break;
                                    builder.append((char) data);
                                }

                                var clobValue = builder.toString().getBytes(StandardCharsets.UTF_8);
                                responseWriter.write((byte)Common.JdbcItemType.TEXT_VALUE);
                                responseWriter.write(Utils.intToBytes(clobValue.length));
                                responseWriter.write(clobValue);
                            } else if (value instanceof Blob) {
                                var blobValue = ((Blob) value).getBinaryStream().readAllBytes();
                                responseWriter.write((byte)Common.JdbcItemType.BINARY_VALUE);
                                responseWriter.write(Utils.intToBytes(blobValue.length));
                                responseWriter.write(blobValue);
                            } else if (value instanceof Array) {
                                var byteString = new StringBuilder("{");

                                var arr = ((Object[]) ((Array) value).getArray());
                                for (Object v : arr) {
                                    if (byteString.length() > 1) {
                                        byteString.append(", ");
                                    }

                                    if (v.getClass() == byte[].class) {
                                        byteString.append(Utils.bytesToHex((byte[]) v));
                                    } else {
                                        byteString.append(v);
                                    }
                                }

                                byteString.append("}");

                                var arrayValue = byteString.toString().getBytes(StandardCharsets.UTF_8);
                                responseWriter.write((byte)Common.JdbcItemType.TEXT_VALUE);
                                responseWriter.write(Utils.intToBytes(arrayValue.length));
                                responseWriter.write(arrayValue);
                            } else {
                                var textValue = value.toString().getBytes(StandardCharsets.UTF_8);
                                responseWriter.write((byte)Common.JdbcItemType.TEXT_VALUE);
                                responseWriter.write(Utils.intToBytes(textValue.length));
                                responseWriter.write(textValue);
                            }
                        }

                        responseWriter.flush();
                        inMemoryStream.flush();

                        responseBuilder.setRows(UnsafeByteOperations.unsafeWrap(inMemoryStream.toByteArray()));
                        inMemoryStream.close();

                        if (readCount >= readResultSetRequest.getChunkSize()) {
                            responseBuilder.setIsCompleted(!resultSet.next());
                            responseObserver.onNext(responseBuilder.build());
                            return;
                        }
                    } while (resultSet.next());

                    if (readCount > 0) {
                        responseBuilder.setIsCompleted(true);
                        responseObserver.onNext(responseBuilder.build());
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
