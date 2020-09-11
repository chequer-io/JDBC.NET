package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.google.protobuf.Empty;
import io.grpc.stub.StreamObserver;
import proto.statement.Statement;
import proto.statement.StatementServiceGrpc;

import java.sql.Connection;

public class StatementServiceImpl extends StatementServiceGrpc.StatementServiceImplBase {
    @Override
    public void createStatement(Statement.CreateStatementRequest request, StreamObserver<Statement.CreateStatementResponse> responseObserver) {
        try {
            Connection connection = ObjectManager.getConnection(request.getConnectionId());
            java.sql.Statement statement = connection.createStatement();
            String statementId = ObjectManager.putStatement(statement);

            Statement.CreateStatementResponse response = Statement.CreateStatementResponse.newBuilder()
                    .setStatementId(statementId)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void closeStatement(Statement.CloseStatementRequest request, StreamObserver<Empty> responseObserver) {
        try {
            java.sql.Statement statement = ObjectManager.getStatement(request.getStatementId());
            statement.close();

            ObjectManager.removeStatement(request.getStatementId());

            Empty response = Empty.newBuilder()
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
