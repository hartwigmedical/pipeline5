package hmf.pipeline.runtime;

import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import hmf.patient.RawSequencingOutput;
import hmf.pipeline.Configuration;
import hmf.pipeline.Pipeline;
import hmf.pipeline.adam.ADAMPipelines;
import hmf.pipeline.gatk.GATK4Pipelines;
import hmf.pipeline.gatk.ReadsAndHeader;
import hmf.pipeline.runtime.configuration.YAMLConfiguration;
import hmf.pipeline.runtime.spark.SparkContexts;

public class PipelineRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineRuntime.class);
    private final Configuration configuration;

    private PipelineRuntime(final Configuration configuration) {
        this.configuration = configuration;
    }

    private void start() throws Exception {
        if (configuration.flavour().equals(Configuration.Flavour.ADAM)) {
            adam();
        } else {
            gatk4();
        }
    }

    private void gatk4() throws java.io.IOException {
        LOGGER.info("Starting GATK4 pipeline for patient [{}]", configuration.patientName());
        Pipeline<ReadsAndHeader> gatk4Pipeline = GATK4Pipelines.preProcessing(configuration, SparkContexts.create("GATK4", configuration));
        gatk4Pipeline.execute(RawSequencingOutput.from(configuration));
        LOGGER.info("Completed GATK4 pipeline for patient [{}]", configuration.patientName());
    }

    private void adam() throws java.io.IOException {
        LOGGER.info("Starting ADAM pipeline for patient [{}]", configuration.patientName());
        ADAMContext adamContext = new ADAMContext(SparkContexts.create("ADAM", configuration).sc());
        Pipeline<AlignmentRecordRDD> adamPipeline = ADAMPipelines.preProcessing(configuration, adamContext);
        adamPipeline.execute(RawSequencingOutput.from(configuration));
        LOGGER.info("Completed ADAM pipeline for patient [{}]", configuration.patientName());
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = YAMLConfiguration.from(System.getProperty("user.dir"));
            new PipelineRuntime(configuration).start();
        } catch (Exception e) {
            LOGGER.error("Fatal error while running ADAM pipeline. See stack trace for more details", e);
        }
    }
}
