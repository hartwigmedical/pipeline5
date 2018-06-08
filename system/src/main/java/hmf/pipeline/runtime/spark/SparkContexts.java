package hmf.pipeline.runtime.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import hmf.pipeline.Configuration;

public class SparkContexts {

    public static JavaSparkContext create(String appName, Configuration configuration) {
        SparkConf conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .setMaster(configuration.sparkMaster())
                .setAppName(appName);
        return new JavaSparkContext(conf);
    }
}
