package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.Common;
import proto.reader.Reader;
import proto.reader.ReaderServiceGrpc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

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
                    while (resultSet.next()) {
                        readCount++;
                        var rowBuilder = Common.JdbcDataRow.newBuilder();

                        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++ ) {
                            var item = Common.JdbcDataItem.newBuilder();
                            var value = resultSet.getString(i);

                            if (value == null) {
                                item.setIsNull(true);
                                value = "";
                            }

                            rowBuilder.addItems(item.setValue(value).build());
                        }

                        responseBuilder.addRows(rowBuilder.build());

                        if (readCount >= readResultSetRequest.getChunkSize()){
                            responseObserver.onNext(responseBuilder.build());
                            return;
                        }
                    }

                    if (readCount > 0) {
                        responseBuilder.setIsCompleted(true);
                        responseObserver.onNext(responseBuilder.build());
                    }

                    onCompleted();
                } catch (Exception e) {
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
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }
}
