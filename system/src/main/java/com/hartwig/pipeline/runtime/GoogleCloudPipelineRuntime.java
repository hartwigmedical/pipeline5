package com.hartwig.pipeline.runtime;

import com.hartwig.pipeline.runtime.configuration.BwaParameters;
import com.hartwig.pipeline.runtime.configuration.Configuration;
import com.hartwig.pipeline.runtime.configuration.KnownIndelParameters;
import com.hartwig.pipeline.runtime.configuration.KnownSnpParameters;
import com.hartwig.pipeline.runtime.configuration.PatientParameters;
import com.hartwig.pipeline.runtime.configuration.PipelineParameters;
import com.hartwig.pipeline.runtime.configuration.ReferenceGenomeParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleCloudPipelineRuntime {

    private static final Logger LOGGER = LoggerFactory.getLogger(GoogleCloudPipelineRuntime.class);

    public static void main(String[] args) {
        if (args.length == 4) {
            String version = args[0];
            String runId = args[1];
            String project = args[2];
            String namespace = args[3];
            LOGGER.info("Starting pipeline with version [{}] run id [{}] for project [{}] in namespace [{}] on Google Dataproc",
                    version,
                    runId,
                    project,
                    namespace);

            Configuration configuration = Configuration.builder()
                    .pipeline(PipelineParameters.builder()
                            .hdfs("gs:///")
                            .bwa(BwaParameters.builder().threads(1).build())
                            .resultsDirectory(namespace + "/results")
                            .build())
                    .referenceGenome(ReferenceGenomeParameters.builder().file("reference.fasta").directory(namespace + "/reference_genome").build())
                    .knownIndel(KnownIndelParameters.builder()
                            .directory(namespace + "/known_indels")
                            .addFiles("1000G_phase1.indels.b37.vcf.gz", "Mills_and_1000G_gold_standard.indels.b37.vcf.gz")
                            .build())
                    .knownSnp(KnownSnpParameters.builder().directory(namespace + "/known_snps").addFiles("dbsnp_137.b37.vcf").build())
                    .patient(PatientParameters.builder().directory(namespace + "/samples").name("").build())
                    .build();

            new PipelineRuntime(configuration).start();
        }

    }
}
