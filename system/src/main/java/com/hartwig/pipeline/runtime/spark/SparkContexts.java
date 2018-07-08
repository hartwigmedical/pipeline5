package com.hartwig.pipeline.runtime.spark;

import java.util.Map;

import com.hartwig.pipeline.runtime.configuration.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContexts {

    public static JavaSparkContext create(String appName, Configuration configuration) {
        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.ui.showConsoleProgress", "false")
                .set("spark.kryoserializer.buffer.max", "1024m")
                .set("spark.ui.showConsoleProgress", "false")
                .set("spark.driver.maxResultSize", "0")
                .setMaster(configuration.spark().get("master"))
                .setAppName(appName);
        for (Map.Entry<String, String> sparkProperty : configuration.spark().entrySet()) {
            conf.set(sparkProperty.getKey(), sparkProperty.getValue());
        }
        return new JavaSparkContext(conf);
    }
}
