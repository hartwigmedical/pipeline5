package hmf.pipeline.gatk;

import org.apache.spark.api.java.JavaSparkContext;

import hmf.pipeline.Stage;
import hmf.sample.Lane;
import hmf.sample.Reference;

class GATK4Stages {

    static Stage<Lane> ubamFromFastQ() {
        return new UBAMFromFastQ();
    }

    static Stage<Lane> coordinateSortSAM() {
        return new CoordinateSortSAM();
    }

    static Stage<Lane> bwaSpark(Reference reference, JavaSparkContext context) {
        return new BwaSpark(reference, context);
    }
}
