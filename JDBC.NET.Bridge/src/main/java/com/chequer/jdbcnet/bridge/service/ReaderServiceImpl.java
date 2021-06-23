package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.chequer.jdbcnet.bridge.utils.Utils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.Common;
import proto.reader.Reader;
import proto.reader.ReaderServiceGrpc;

import java.sql.*;

public class ReaderServiceImpl extends ReaderServiceGrpc.ReaderServiceImplBase {
    @Override
    public StreamObserver<Reader.ReadResultSetRequest> readResultSet(final StreamObserver<Reader.ReadResultSetResponse> responseObserver) {
        return new StreamObserver<Reader.ReadResultSetRequest>() {
            public void onNext(Reader.ReadResultSetRequest readResultSetRequest) {
                try {
                    var resultSet = ObjectManager.getResultSet(readResultSetRequest.getResultSetId());
                    var resultSetMetaData = resultSet.getMetaData();
                    var responseBuilder = Reader.ReadResultSetResponse.newBuilder();

                    int readCount = 0;
                    do {
                        if (!resultSet.getHasRows())
                            break;

                        readCount++;
                        var rowBuilder = Common.JdbcDataRow.newBuilder();

                        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                            var item = Common.JdbcDataItem.newBuilder();
                            var value = resultSet.getObject(i);

                            if (value == null) {
                                item.setIsNull(true);
                            } else if (value.getClass() == byte[].class) {
                                item.setByteArray(ByteString.copyFrom((byte[]) value));
                            } else if (value instanceof Clob) {
                                var reader = ((Clob) value).getCharacterStream();

                                var builder = new StringBuilder();

                                while (true) {
                                    var data = reader.read();
                                    if (data == -1) break;
                                    builder.append((char) data);
                                }

                                item.setText(builder.toString());
                            } else if (value instanceof Blob) {
                                item.setByteArray(ByteString.copyFrom(((Blob) value).getBinaryStream().readAllBytes()));
                            } else if (value instanceof Array) {
                                var byteString = ByteString.copyFromUtf8("{");

                                var arr = ((Object[]) ((Array) value).getArray());
                                for (Object v : arr) {
                                    if (byteString.size() > 1) {
                                        byteString = byteString.concat(ByteString.copyFromUtf8(", "));
                                    }

                                    if (v.getClass() == byte[].class) {
                                        byteString = byteString.concat(ByteString.copyFromUtf8(Utils.bytesToHex((byte[]) v)));
                                    } else {
                                        byteString = byteString.concat(ByteString.copyFromUtf8(v.toString()));
                                    }
                                }

                                byteString = byteString.concat(ByteString.copyFromUtf8("}"));

                                item.setTextBytes(byteString);
                            } else {
                                item.setText(value.toString());
                            }

                            rowBuilder.addItems(item.build());
                        }

                        responseBuilder.addRows(rowBuilder.build());

                        if (readCount >= readResultSetRequest.getChunkSize()) {
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
