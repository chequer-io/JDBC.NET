package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import io.grpc.stub.StreamObserver;
import proto.Common;
import proto.reader.Reader;
import proto.reader.ReaderServiceGrpc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class ReaderServiceImpl extends ReaderServiceGrpc.ReaderServiceImplBase {
    @Override
    public void readResultSet(Reader.ReadResultSetRequest request, StreamObserver<Common.JdbcDataRow> responseObserver) {
        try {
            ResultSet resultSet = ObjectManager.getResultSet(request.getResultSetId());
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            while (resultSet.next()) {
                Common.JdbcDataRow.Builder responseBuilder = Common.JdbcDataRow.newBuilder();

                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++ ) {
                    responseBuilder.addItems(resultSet.getString(i));
                }

                responseObserver.onNext(responseBuilder.build());
            }

            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
