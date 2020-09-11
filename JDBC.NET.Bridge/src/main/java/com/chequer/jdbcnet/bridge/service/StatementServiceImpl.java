package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.Common;
import proto.statement.Statement;
import proto.statement.StatementServiceGrpc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

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
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void executeStatement(Statement.ExecuteStatementRequest request, StreamObserver<Statement.ExecuteStatementResponse> responseObserver) {
        try {
            java.sql.Statement statement = ObjectManager.getStatement(request.getStatementId());
            ResultSet resultSet = statement.executeQuery(request.getSql());
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            String resultSetId = ObjectManager.putResultSet(resultSet);

            Statement.ExecuteStatementResponse.Builder responseBuilder = Statement.ExecuteStatementResponse.newBuilder()
                    .setResultSetId(resultSetId)
                    .setRecordsAffected(statement.getUpdateCount())
                    .setHasRows(resultSet.isBeforeFirst());

            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++ ) {
                responseBuilder.addColumns(Common.JdbcDataColumn.newBuilder()
                        .setOrdinal(i - 1)
                        .setColumnName(resultSetMetaData.getColumnName(i))
                        .setDataTypeName(resultSetMetaData.getColumnTypeName(i))
                        .setDataTypeCode(resultSetMetaData.getColumnType(i))
                        .build());
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
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
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }
}
