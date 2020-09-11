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
import java.sql.SQLException;

public class ReaderServiceImpl extends ReaderServiceGrpc.ReaderServiceImplBase {
    @Override
    public StreamObserver<Reader.ReadResultSetRequest> readResultSet(final StreamObserver<Common.JdbcDataRow> responseObserver) {
        return new StreamObserver<Reader.ReadResultSetRequest>() {
            public void onNext(Reader.ReadResultSetRequest readResultSetRequest) {
                try {
                    ResultSet resultSet = ObjectManager.getResultSet(readResultSetRequest.getResultSetId());
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

                    if (resultSet.next()) {
                        Common.JdbcDataRow.Builder responseBuilder = Common.JdbcDataRow.newBuilder();

                        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++ ) {
                            responseBuilder.addItems(resultSet.getString(i));
                        }

                        responseObserver.onNext(responseBuilder.build());
                    }
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
            ResultSet resultSet = ObjectManager.getResultSet(request.getResultSetId());
            resultSet.close();

            ObjectManager.removeResultSet(request.getResultSetId());

            Empty response = Empty.newBuilder()
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
