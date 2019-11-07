package com.hartwig.batch;

import static java.lang.String.format;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface InstanceFactory<T> {
    Logger LOGGER = LoggerFactory.getLogger(InstanceFactory.class);

    BatchOperation get();

    static InstanceFactory from(BatchArguments arguments) {
        Map<BatchOperation, Constructor> availableOperations = findBatchOperations();
        for (BatchOperation operation : availableOperations.keySet()) {
            if (operation.descriptor().callName().equals(arguments.verb().trim())) {
                LOGGER.info(format("Found [%s] to provide [%s]", operation.getClass().getName(), arguments.verb()));
                return () -> {
                    try {
                        return (BatchOperation) availableOperations.get(operation).newInstance();
                    } catch (Exception e) {
                        throw new RuntimeException(format("Failed to build [%s] instance!", e));
                    }
                };
            }
        }
        throw new IllegalArgumentException(format("Could not find a class to handle requested action [%s]", arguments.verb()));
    }

    private static Map<BatchOperation, Constructor> findBatchOperations() {
        Map<BatchOperation, Constructor> availableOperations = new HashMap<>();
        Reflections scanner = new Reflections(ClasspathHelper.forPackage("com.hartwig.batch"), new SubTypesScanner(false));
        for (Class<?> subType : scanner.getSubTypesOf(Object.class)) {
            if (Arrays.asList(subType.getInterfaces()).contains(BatchOperation.class)) {
                try {
                    Constructor<?> constructor = subType.getConstructor();
                    availableOperations.put((BatchOperation) constructor.newInstance(), constructor);
                } catch (NoSuchMethodException e) {
                    throw new RuntimeException(format("Failed to instantiate [%s] using no-argument constructor", subType.getName()), e);
                } catch (Exception e) {
                    throw new RuntimeException(format("Found constructor for [%s] but calling it failed", subType.getName()), e);
                }
            }
        }
        return availableOperations;
    }
}
