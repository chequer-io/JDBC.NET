package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import io.grpc.stub.StreamObserver;
import proto.reader.Reader;
import proto.reader.ReaderServiceGrpc;

import java.sql.ResultSet;

public class ReaderServiceImpl extends ReaderServiceGrpc.ReaderServiceImplBase {
    @Override
    public void readResultSet(Reader.ReadResultSetRequest request, StreamObserver<Reader.ReadResultSetResponse> responseObserver) {
        try {
            ResultSet resultSet = ObjectManager.getResultSet(request.getResultSetId());

            while (resultSet.next()) {
                Reader.ReadResultSetResponse response = Reader.ReadResultSetResponse.newBuilder()
                        .setValue(resultSet.getString(1))
                        .build();

                responseObserver.onNext(response);
            }

            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
