package hmf.pipeline.runtime.adam;

import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hmf.patient.RawSequencingOutput;
import hmf.pipeline.Configuration;
import hmf.pipeline.Pipeline;
import hmf.pipeline.adam.ADAMPipelines;
import hmf.pipeline.runtime.configuration.YAMLConfiguration;
import hmf.pipeline.runtime.spark.SparkContexts;

public class ADAMRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(ADAMRuntime.class);
    private final Configuration configuration;

    private ADAMRuntime(final Configuration configuration) {
        this.configuration = configuration;
    }

    private void start() throws Exception {
        LOGGER.info("Starting ADAM pipeline for patient [{}]", configuration.patientName());
        ADAMContext adamContext = new ADAMContext(SparkContexts.create("ADAM", configuration).sc());
        Pipeline<AlignmentRecordRDD> adamPipeline = ADAMPipelines.preProcessing(configuration, adamContext);
        adamPipeline.execute(RawSequencingOutput.from(configuration));
        LOGGER.info("Completed ADAM pipeline for patient [{}]", configuration.patientName());
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = YAMLConfiguration.from(System.getProperty("user.dir"));
            new ADAMRuntime(configuration).start();
        } catch (Exception e) {
            LOGGER.error("Fatal error while running ADAM pipeline. See stack trace for more details", e);
        }
    }
}
