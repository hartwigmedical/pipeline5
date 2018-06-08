package hmf.pipeline.runtime.gatk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hmf.patient.RawSequencingOutput;
import hmf.pipeline.Configuration;
import hmf.pipeline.Pipeline;
import hmf.pipeline.gatk.GATK4Pipelines;
import hmf.pipeline.gatk.ReadsAndHeader;
import hmf.pipeline.runtime.configuration.YAMLConfiguration;
import hmf.pipeline.runtime.spark.SparkContexts;

public class GATK4Runtime {

    private static final Logger LOGGER = LoggerFactory.getLogger(GATK4Runtime.class);
    private final Configuration configuration;

    private GATK4Runtime(final Configuration configuration) {
        this.configuration = configuration;
    }

    private void start() throws Exception {
        LOGGER.info("Starting GATK4 pipeline for patient [{}]", configuration.patientName());
        Pipeline<ReadsAndHeader> gatk4Pipeline = GATK4Pipelines.preProcessing(configuration, SparkContexts.create("GATK4", configuration));
        gatk4Pipeline.execute(RawSequencingOutput.from(configuration));
        LOGGER.info("Completed GATK4 pipeline for patient [{}]", configuration.patientName());
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = YAMLConfiguration.from(System.getProperty("user.dir"));
            new GATK4Runtime(configuration).start();
        } catch (Exception e) {
            LOGGER.error("Fatal error while running GATK4 pipeline. See stack trace for more details", e);
        }
    }
}
