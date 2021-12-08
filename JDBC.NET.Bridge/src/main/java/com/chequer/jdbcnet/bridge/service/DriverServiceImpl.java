package com.chequer.jdbcnet.bridge.service;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.driver.Driver;
import proto.driver.DriverServiceGrpc;

import java.sql.DriverManager;

public class DriverServiceImpl extends DriverServiceGrpc.DriverServiceImplBase {
    @Override
    public void loadDriver(Driver.LoadDriverRequest request, StreamObserver<Driver.LoadDriverResponse> responseObserver) {
        try {
            // Load Class from driver
            var driver = getDriverByClass(Class.forName(request.getClassName()));
            System.out.println("Driver class '" + request.getClassName() + "' loaded successfully.");

            // Return response
            var response = Driver.LoadDriverResponse.newBuilder()
                    .setMajorVersion(driver.getMajorVersion())
                    .setMinorVersion(driver.getMinorVersion())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Throwable e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    public java.sql.Driver getDriverByClass(Class clazz) throws ClassNotFoundException {
        var drivers = DriverManager.getDrivers();

        while(drivers.hasMoreElements()) {
            var current = drivers.nextElement();

            if (current.getClass() == clazz)
                return current;
        }

        throw new ClassNotFoundException();
    }
}
