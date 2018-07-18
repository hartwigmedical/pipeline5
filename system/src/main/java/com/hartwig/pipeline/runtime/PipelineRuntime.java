package com.hartwig.pipeline.runtime;

import com.hartwig.pipeline.Pipeline;
import com.hartwig.pipeline.adam.ADAMPipelines;
import com.hartwig.pipeline.gatk.GATK4Pipelines;
import com.hartwig.pipeline.gatk.ReadsAndHeader;
import com.hartwig.pipeline.runtime.configuration.Configuration;
import com.hartwig.pipeline.runtime.configuration.YAMLConfigurationReader;
import com.hartwig.pipeline.runtime.patient.PatientReader;
import com.hartwig.pipeline.runtime.spark.SparkContexts;

import org.bdgenomics.adam.rdd.ADAMContext;
import org.bdgenomics.adam.rdd.read.AlignmentRecordRDD;
import org.bdgenomics.adam.rdd.variant.VariantContextRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineRuntime.class);
    private final Configuration configuration;

    private PipelineRuntime(final Configuration configuration) {
        this.configuration = configuration;
    }

    private void start() throws Exception {
        if (configuration.pipeline().flavour().equals(Configuration.Flavour.ADAM)) {
            adam();
        } else {
            gatk4();
        }
    }

    private void gatk4() throws java.io.IOException {
        LOGGER.info("Starting GATK4 pipeline for patient [{}]", configuration.patient().name());
        Pipeline<ReadsAndHeader, ReadsAndHeader> gatk4Pipeline =
                GATK4Pipelines.preProcessing(configuration.referenceGenome().path(), SparkContexts.create("GATK4", configuration));
        gatk4Pipeline.execute(PatientReader.from(configuration));
        LOGGER.info("Completed GATK4 pipeline for patient [{}]", configuration.patient().name());
    }

    private void adam() throws java.io.IOException {
        LOGGER.info("Starting ADAM pipeline for patient [{}]", configuration.patient().name());
        ADAMContext adamContext = new ADAMContext(SparkContexts.create("ADAM", configuration).sc());
        Pipeline<AlignmentRecordRDD, VariantContextRDD> adamPipeline =
                ADAMPipelines.preProcessing(configuration.referenceGenome().path(), configuration.knownIndel().paths(),
                        adamContext,
                        configuration.pipeline().bwa().threads(),
                        configuration.pipeline().callGermline());
        adamPipeline.execute(PatientReader.from(configuration));
        LOGGER.info("Completed ADAM pipeline for patient [{}]", configuration.patient().name());
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = YAMLConfigurationReader.from(System.getProperty("user.dir"));
            new PipelineRuntime(configuration).start();
        } catch (Exception e) {
            LOGGER.error("Fatal error while running ADAM pipeline. See stack trace for more details", e);
        }
    }
}
