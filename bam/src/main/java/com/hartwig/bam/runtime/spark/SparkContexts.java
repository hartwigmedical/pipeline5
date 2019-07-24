package com.hartwig.bam.runtime.spark;

import java.util.Map;

import com.hartwig.bam.ADAMKryo;
import com.hartwig.bam.runtime.configuration.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkContexts {

    public static JavaSparkContext create(String appName, Configuration configuration) {
        return create(appName, configuration.spark().get("master"), configuration.spark());
    }

    public static JavaSparkContext create(String appName, String master, Map<String, String> sparkProperties) {
        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.ui.showConsoleProgress", "false")
                .set("spark.kryoserializer.buffer.max", "2046m")
                .set("spark.kryo.referenceTracking", "false").set("spark.kryo.registrator", ADAMKryo.class.getName())
                .set("spark.kryo.registrationRequired", "true")
                .set("spark.driver.maxResultSize", "0")
                .setAppName(appName);
        if (master != null) {
            conf.setMaster(master);
        }
        sparkProperties.forEach(conf::set);
        return new JavaSparkContext(conf);
    }
}