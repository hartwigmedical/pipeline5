package com.hartwig.pipeline.runtime;

import java.io.IOException;

import com.hartwig.pipeline.BamCreationPipeline;
import com.hartwig.pipeline.adam.ADAMPipelines;
import com.hartwig.pipeline.runtime.configuration.Configuration;
import com.hartwig.pipeline.runtime.configuration.YAMLConfigurationReader;
import com.hartwig.pipeline.runtime.hadoop.Hadoop;
import com.hartwig.pipeline.runtime.patient.PatientReader;
import com.hartwig.pipeline.runtime.spark.SparkContexts;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkContext;
import org.bdgenomics.adam.rdd.ADAMContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineRuntime.class);
    private final Configuration configuration;

    private PipelineRuntime(final Configuration configuration) {
        this.configuration = configuration;
    }

    private void start() {
        SparkContext sparkContext = SparkContexts.create("ADAM", configuration).sc();
        ;
        try {
            FileSystem fileSystem = Hadoop.fileSystem(configuration);
            ADAMContext adamContext = new ADAMContext(sparkContext);
            BamCreationPipeline adamPipeline = ADAMPipelines.bamCreation(adamContext,
                    fileSystem,
                    configuration.pipeline().resultsDirectory(),
                    configuration.referenceGenome().path(),
                    configuration.knownIndel().paths(),
                    configuration.pipeline().bwa().threads(),
                    false,
                    true,
                    false);
            adamPipeline.execute(PatientReader.fromHDFS(fileSystem, configuration));
        } catch (Exception e) {
            LOGGER.error("Fatal error while running ADAM pipeline. See stack trace for more details", e);
        } finally {
            LOGGER.info("Pipeline complete, stopping spark context");
            sparkContext.stop();
            LOGGER.info("Spark context stopped");
        }
        System.exit(0);
    }

    public static void main(String[] args) {
        Configuration configuration = null;
        try {
            configuration = YAMLConfigurationReader.from(System.getProperty("user.dir"));
        } catch (IOException e) {
            LOGGER.error("Unable to read configuration. Check configuration in /conf/pipeline.yaml", e);
        }
        new PipelineRuntime(configuration).start();
    }
}
