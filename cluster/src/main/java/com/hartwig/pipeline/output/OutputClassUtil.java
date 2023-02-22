package com.hartwig.pipeline.output;

public class OutputClassUtil {

    private OutputClassUtil() {
    }

    public static <T> String getOutputClassTag(Class<T> clazz) {
        return clazz.getSimpleName();
    }

    public static <T> String getOutputClassTag(Class<T> clazz, String label) {
        return String.format("%s-%s", clazz.getSimpleName(), label);
    }
}
