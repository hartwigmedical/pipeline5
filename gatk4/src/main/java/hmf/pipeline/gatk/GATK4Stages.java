package hmf.pipeline.gatk;

import org.apache.spark.api.java.JavaSparkContext;

import hmf.pipeline.Configuration;
import hmf.pipeline.Stage;

class GATK4Stages {

    static Stage ubamFromFastQ(Configuration configuration) {
        return new UBAMFromFastQ(configuration);
    }

    static Stage coordinateSortSAM(Configuration configuration) {
        return new CoordinateSortSAM(configuration);
    }

    static Stage bwaSpark(Configuration configuration, JavaSparkContext context) {
        return new BwaSpark(configuration, context);
    }
}
