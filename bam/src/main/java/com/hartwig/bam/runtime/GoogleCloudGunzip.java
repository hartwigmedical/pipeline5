package com.hartwig.bam.runtime;

import com.hartwig.bam.HadoopStatusReporter;
import com.hartwig.bam.StatusReporter;
import com.hartwig.bam.before.GunZip;
import com.hartwig.bam.runtime.configuration.Configuration;
import com.hartwig.bam.runtime.configuration.PatientParameters;
import com.hartwig.bam.runtime.configuration.PipelineParameters;
import com.hartwig.bam.runtime.configuration.ReferenceGenomeParameters;
import com.hartwig.bam.runtime.spark.SparkContexts;
import com.hartwig.patient.Patient;
import com.hartwig.patient.input.PatientReader;
import com.hartwig.support.hadoop.Hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudGunzip {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudGunzip.class);

    private final Configuration configuration;
    private final String name;

    private GoogleCloudGunzip(final Configuration configuration, String name) {
        this.configuration = configuration;
        this.name = name;
    }

    private void execute() {
        JavaSparkContext javaSparkContext = SparkContexts.create("ADAM", configuration);
        SparkContext sparkContext = javaSparkContext.sc();
        try {
            FileSystem fileSystem = Hadoop.fileSystem(configuration.pipeline().hdfs());
            Patient patient = PatientReader.fromHDFS(fileSystem, configuration.patient().directory(), configuration.patient().name());
            StatusReporter statusReporter = new HadoopStatusReporter(fileSystem, configuration.pipeline().resultsDirectory(), name);
            GunZip.execute(fileSystem, javaSparkContext, patient.reference(), false, statusReporter);
        } catch (Exception e) {
            LOGGER.error("Fatal error while running ADAM pipeline. See stack trace for more details", e);
            throw new RuntimeException(e);
        } finally {
            LOGGER.info("Pipeline complete, stopping spark context");
            sparkContext.stop();
            LOGGER.info("Spark context stopped");
        }
        System.exit(0);
    }

    public static void main(String[] args) {
        String namespace = args[0];
        String name = args[1];
        Configuration configuration = Configuration.builder()
                .pipeline(PipelineParameters.builder()
                        .hdfs("gs:///")
                        .resultsDirectory(namespace + "/results")
                        .build())
                .referenceGenome(ReferenceGenomeParameters.builder().file("N/A").build())
                .patient(PatientParameters.builder().directory(namespace + "/samples").name("").build())
                .build();
        new GoogleCloudGunzip(configuration, name).execute();
    }
}
