package com.hartwig.pipeline.runtime;

import com.hartwig.pipeline.metrics.Monitor;
import com.hartwig.pipeline.metrics.Run;
import com.hartwig.pipeline.runtime.configuration.BwaParameters;
import com.hartwig.pipeline.runtime.configuration.Configuration;
import com.hartwig.pipeline.runtime.configuration.KnownIndelParameters;
import com.hartwig.pipeline.runtime.configuration.PatientParameters;
import com.hartwig.pipeline.runtime.configuration.PipelineParameters;
import com.hartwig.pipeline.runtime.configuration.ReferenceGenomeParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudPipelineRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudPipelineRuntime.class);

    public static void main(String[] args) {
        Configuration configuration = Configuration.builder()
                .pipeline(PipelineParameters.builder().hdfs("gs:///").bwa(BwaParameters.builder().threads(1).build()).build())
                .referenceGenome(ReferenceGenomeParameters.builder().file("reference.fasta").build())
                .knownIndel(KnownIndelParameters.builder()
                        .addFiles("1000G_phase1.indels.b37.vcf.gz", "Mills_and_1000G_gold_standard.indels.b37.vcf.gz")
                        .build()).patient(PatientParameters.builder().directory("/samples").name("").build())
                .build();
        Monitor monitor = Monitor.noop();
        if (args.length == 3) {
            String version = args[0];
            String runId = args[1];
            String project = args[2];
            LOGGER.info("Starting pipeline with version [{}] run id [{}] for project [{}] on Google Dataproc", version, runId, project);
            monitor = Monitor.stackdriver(Run.of(version, runId), project);
        }
        new PipelineRuntime(configuration, monitor, false).start();
    }
}
