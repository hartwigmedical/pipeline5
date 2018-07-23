package com.hartwig.testsupport;

import com.hartwig.pipeline.runtime.spark.SparkContexts;

import org.apache.spark.api.java.JavaSparkContext;

public class SparkContextSingleton {

    private static JavaSparkContext CONTEXT;

    public static JavaSparkContext instance() {
        if (CONTEXT == null) {
            CONTEXT = SparkContexts.create("test", TestConfigurations.HUNDREDK_READS_HISEQ);
        }
        return CONTEXT;
    }

}
