package com.hartwig.pipeline.input;

public interface InputDependencyProvider {
    <T> T registerInput(Class<T> clazz);
    <T> T registerInput(Class<T> clazz, String label);
}
