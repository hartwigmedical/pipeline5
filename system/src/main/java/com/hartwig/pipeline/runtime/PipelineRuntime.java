package com.hartwig.pipeline.runtime;

import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.adam.ADAMPipelines;
import com.hartwig.pipeline.runtime.configuration.Configuration;
import com.hartwig.pipeline.runtime.configuration.YAMLConfigurationReader;
import com.hartwig.pipeline.runtime.patient.PatientReader;
import com.hartwig.pipeline.runtime.spark.SparkContexts;

import org.bdgenomics.adam.rdd.ADAMContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineRuntime.class);
    private final Configuration configuration;

    private PipelineRuntime(final Configuration configuration) {
        this.configuration = configuration;
    }

    private void start() throws Exception {
        LOGGER.info("Starting ADAM pipeline for patient [{}]", configuration.patient().name());
        ADAMContext adamContext = new ADAMContext(SparkContexts.create("ADAM", configuration).sc());
        BamCreationPipeline adamPipeline = ADAMPipelines.bamCreation(configuration.referenceGenome().path(),
                configuration.knownIndel().paths(),
                adamContext, configuration.pipeline().bwa().threads());
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
