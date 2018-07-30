package com.hartwig.testsupport;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.runtime.configuration.Configuration;
import com.hartwig.pipeline.runtime.configuration.ImmutableConfiguration;
import com.hartwig.pipeline.runtime.configuration.ImmutableKnownIndelParameters;
import com.hartwig.pipeline.runtime.configuration.ImmutablePatientParameters;
import com.hartwig.pipeline.runtime.configuration.ImmutablePipelineParameters;
import com.hartwig.pipeline.runtime.configuration.ImmutableReferenceGenomeParameters;
import com.hartwig.pipeline.runtime.configuration.KnownIndelParameters;
import com.hartwig.pipeline.runtime.configuration.ReferenceGenomeParameters;

public class TestConfigurations {
    public static final String PATIENT_DIR = "/src/test/resources/patients";

    public static final String HUNDREDK_READS_HISEQ_PATIENT_NAME = "TESTX";

    public static final ReferenceGenomeParameters REFERENCE_GENOME_PARAMETERS = ImmutableReferenceGenomeParameters.builder()
            .directory(System.getProperty("user.dir") + "/src/test/resources/reference_genome/")
            .file("Homo_sapiens.GRCh37.GATK.illumina.chr22.fa")
            .build();

    private static final KnownIndelParameters KNOWN_INDEL_PARAMETERS = ImmutableKnownIndelParameters.builder()
            .directory(System.getProperty("user.dir") + "/src/test/resources/known_indels/")
            .addFiles("1000G_phase1.indels.b37.vcf.gz")
            .build();

    public static final ImmutablePatientParameters.Builder DEFAULT_PATIENT_BUILDER = ImmutablePatientParameters.builder();

    public static final ImmutableConfiguration.Builder DEFAULT_CONFIG_BUILDER = ImmutableConfiguration.builder()
            .spark(ImmutableMap.of("master", "local[2]"))
            .pipeline(ImmutablePipelineParameters.builder().build())
            .referenceGenome(REFERENCE_GENOME_PARAMETERS)
            .knownIndel(KNOWN_INDEL_PARAMETERS);

    public static final Configuration HUNDREDK_READS_HISEQ = DEFAULT_CONFIG_BUILDER.patient(DEFAULT_PATIENT_BUILDER.directory(
            System.getProperty("user.dir") + PATIENT_DIR + "/100k_reads_hiseq").name(HUNDREDK_READS_HISEQ_PATIENT_NAME).build()).build();
}
