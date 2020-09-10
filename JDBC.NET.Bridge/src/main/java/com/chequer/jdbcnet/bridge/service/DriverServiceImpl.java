package com.chequer.jdbcnet.bridge.service;

import driver.Driver;
import driver.DriverServiceGrpc;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

public class DriverServiceImpl extends DriverServiceGrpc.DriverServiceImplBase {
    @Override
    public void loadDriver(Driver.LoadDriverRequest request, StreamObserver<Driver.LoadDriverResponse> responseObserver) {
        try {
            // Load JAR from path
            File file = new File(request.getPath());
            URLClassLoader classLoader = (URLClassLoader)ClassLoader.getSystemClassLoader();
            Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
            method.setAccessible(true);
            method.invoke(classLoader, file.toURI().toURL());

            // Load Class from driver
            Class clazz = Class.forName(request.getClassName());
            java.sql.Driver driver = (java.sql.Driver)clazz.newInstance();

            // Return response
            Driver.LoadDriverResponse response = Driver.LoadDriverResponse.newBuilder()
                    .setMajorVersion(driver.getMajorVersion())
                    .setMinorVersion(driver.getMinorVersion())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
