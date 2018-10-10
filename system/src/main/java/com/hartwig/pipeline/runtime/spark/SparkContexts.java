package com.hartwig.pipeline.runtime.spark;

import com.hartwig.pipeline.runtime.configuration.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContexts {

    public static JavaSparkContext create(String appName, Configuration configuration) {
        String master = configuration.spark().get("master");
        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.ui.showConsoleProgress", "false")
                .set("spark.kryoserializer.buffer.max", "1024m").set("spark.kryo.referenceTracking", "false")
                .set("spark.ui.showConsoleProgress", "false")
                .set("spark.driver.maxResultSize", "0")
                .setAppName(appName);
        if (master != null) {
            conf.setMaster(master);
        }
        configuration.spark().forEach(conf::set);
        return new JavaSparkContext(conf);
    }
}
