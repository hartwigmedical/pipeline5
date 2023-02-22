package com.hartwig.pipeline.input;

public class InputDependencyCollector implements InputDependencyProvider {

    @Override
    public <T> T registerInput(Class<T> clazz) {
        return null;
    }

    @Override
    public <T> T registerInput(Class<T> clazz, String label) {
        return null;
    }
}
