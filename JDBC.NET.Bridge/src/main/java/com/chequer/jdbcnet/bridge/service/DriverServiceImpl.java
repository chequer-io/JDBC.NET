package com.chequer.jdbcnet.bridge.service;

import com.chequer.jdbcnet.bridge.reflection.RuntimeDriver;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import proto.driver.Driver;
import proto.driver.DriverServiceGrpc;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.DriverManager;

public class DriverServiceImpl extends DriverServiceGrpc.DriverServiceImplBase {
    @Override
    public void loadDriver(Driver.LoadDriverRequest request, StreamObserver<Driver.LoadDriverResponse> responseObserver) {
        try {
            // Load JAR from path
            File file = new File(request.getPath());
            URLClassLoader classLoader = new URLClassLoader(new URL[]{file.toURI().toURL()});

            // Load Class from driver
            Class<?> clazz = Class.forName(request.getClassName(), true, classLoader);
            java.sql.Driver driver = (java.sql.Driver) clazz.getDeclaredConstructor().newInstance();
            DriverManager.registerDriver(new RuntimeDriver(driver));

            // Return response
            Driver.LoadDriverResponse response = Driver.LoadDriverResponse.newBuilder()
                    .setMajorVersion(driver.getMajorVersion())
                    .setMinorVersion(driver.getMinorVersion())
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
