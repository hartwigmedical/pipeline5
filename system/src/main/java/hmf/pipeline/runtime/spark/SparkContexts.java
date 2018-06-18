package hmf.pipeline.runtime.spark;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import hmf.pipeline.Configuration;

public class SparkContexts {

    public static JavaSparkContext create(String appName, Configuration configuration) {
        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster(configuration.sparkMaster())
                .setAppName(appName);
        for (Map.Entry<String, String> sparkProperty : configuration.sparkProperties().entrySet()) {
            conf.set(sparkProperty.getKey(), sparkProperty.getValue());
        }
        return new JavaSparkContext(conf);
    }
}
