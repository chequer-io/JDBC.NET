package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.manager.ObjectManager;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.Common;
import proto.statement.Statement;
import proto.statement.StatementServiceGrpc;

import java.math.BigDecimal;
import java.sql.*;

public class StatementServiceImpl extends StatementServiceGrpc.StatementServiceImplBase {
    @Override
    public void createStatement(Statement.CreateStatementRequest request, StreamObserver<Statement.CreateStatementResponse> responseObserver) {
        try {
            var connection = ObjectManager.getConnection(request.getConnectionId());
            var statement = connection.prepareStatement(request.getSql(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            var statementId = ObjectManager.putStatement(statement);

            var response = Statement.CreateStatementResponse.newBuilder()
                    .setStatementId(statementId)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void executeStatement(Statement.ExecuteStatementRequest request, StreamObserver<Statement.ExecuteStatementResponse> responseObserver) {
        try {
            var statement = ObjectManager.getStatement(request.getStatementId());
            statement.setFetchSize(request.getFetchSize() == -1 ? statement.getMaxRows() : request.getFetchSize());

            if (!statement.execute()) {
                var responseBuilder = Statement.ExecuteStatementResponse.newBuilder()
                        .setRecordsAffected(statement.getUpdateCount());

                responseObserver.onNext(responseBuilder.build());
            } else {
                var resultSet = statement.getResultSet();
                var resultSetMetaData = resultSet.getMetaData();
                var resultSetId = ObjectManager.putResultSet(resultSet);

                var responseBuilder = Statement.ExecuteStatementResponse.newBuilder()
                        .setResultSetId(resultSetId)
                        .setHasRows(resultSet.next());

                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    var columnName = resultSetMetaData.getColumnName(i);
                    var columnLabel = resultSetMetaData.getColumnLabel(i);

                    responseBuilder.addColumns(Common.JdbcDataColumn.newBuilder()
                            .setOrdinal(i - 1)
                            .setTableName(resultSetMetaData.getTableName(i))
                            .setSchemaName(resultSetMetaData.getSchemaName(i))
                            .setCatalogName(resultSetMetaData.getCatalogName(i))
                            .setColumnName(columnName)
                            .setColumnLabel(columnLabel)
                            .setColumnDisplaySize(resultSetMetaData.getColumnDisplaySize(i))
                            .setColumnPrecision(resultSetMetaData.getPrecision(i))
                            .setColumnScale(resultSetMetaData.getScale(i))
                            .setDataTypeName(resultSetMetaData.getColumnTypeName(i))
                            .setDataTypeClassName(resultSetMetaData.getColumnClassName(i))
                            .setDataTypeCode(resultSetMetaData.getColumnType(i))
                            .setIsAutoIncrement(resultSetMetaData.isAutoIncrement(i))
                            .setIsCaseSensitive(resultSetMetaData.isCaseSensitive(i))
                            .setIsDefinitelyWritable(resultSetMetaData.isDefinitelyWritable(i))
                            .setIsSearchable(resultSetMetaData.isSearchable(i))
                            .setIsNullable(resultSetMetaData.isNullable(i))
                            .setIsAliased(!columnName.equals(columnLabel))
                            .setIsWritable(resultSetMetaData.isWritable(i))
                            .setIsCurrency(resultSetMetaData.isCurrency(i))
                            .setIsReadOnly(resultSetMetaData.isReadOnly(i))
                            .setIsSigned(resultSetMetaData.isSigned(i))
                            .build());
                }

                responseObserver.onNext(responseBuilder.build());
            }

            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    @Override
    public void cancelStatement(Statement.CancelStatementRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var statement = ObjectManager.getStatement(request.getStatementId());
            statement.cancel();

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

    @Override
    public void closeStatement(Statement.CloseStatementRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var statement = ObjectManager.getStatement(request.getStatementId());
            statement.close();

            ObjectManager.removeStatement(request.getStatementId());

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

    @Override
    public void setParameter(Statement.SetParameterRequest request, StreamObserver<Empty> responseObserver) {
        try {
            var statement = ObjectManager.getStatement(request.getStatementId());

            var index = request.getIndex();
            var value = request.getValue();

            switch (request.getType()) {
                case INT:
                    statement.setInt(index, Integer.parseInt(value));
                    break;

                case LONG:
                    statement.setLong(index, Long.parseLong(value));
                    break;

                case SHORT:
                    statement.setShort(index, Short.parseShort(value));
                    break;

                case FLOAT:
                    statement.setFloat(index, Float.parseFloat(value));
                    break;

                case DOUBLE:
                    statement.setDouble(index, Double.parseDouble(value));
                    break;

                case STRING:
                    statement.setString(index, value);
                    break;

                case BOOLEAN:
                    statement.setBoolean(index, Boolean.parseBoolean(value));

                case TIME:
                    statement.setTime(index, Time.valueOf(value));
                    break;

                case DATE:
                    statement.setDate(index, Date.valueOf(value));
                    break;
            }

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
