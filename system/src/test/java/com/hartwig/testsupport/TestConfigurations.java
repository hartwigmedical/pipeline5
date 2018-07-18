package com.hartwig.testsupport;

import com.google.common.collect.ImmutableMap;
import com.hartwig.pipeline.runtime.configuration.Configuration;
import com.hartwig.pipeline.runtime.configuration.ImmutableConfiguration;
import com.hartwig.pipeline.runtime.configuration.ImmutablePatientParameters;
import com.hartwig.pipeline.runtime.configuration.ImmutablePipelineParameters;

public class TestConfigurations {
    public static final String PATIENT_DIR = "/src/test/resources/patients";

    public static final String HUNDREDK_READS_HISEQ_PATIENT_NAME = "TESTX";
    public static final ImmutablePatientParameters.Builder DEFAULT_PATIENT_BUILDER = ImmutablePatientParameters.builder()
            .referenceGenomeDirectory(System.getProperty("user.dir") + "/src/test/resources/reference_genome/")
            .referenceGenomeFile("Homo_sapiens.GRCh37.GATK.illumina.chr22.fa")
            .knownIndelDirectory(System.getProperty("user.dir") + "/src/test/resources/known_indels/")
            .addKnownIndelFiles("1000G_phase1.indels.b37.vcf.gz");

    public static final ImmutableConfiguration.Builder DEFAULT_CONFIG_BUILDER = ImmutableConfiguration.builder()
            .spark(ImmutableMap.of("master", "local[2]"))
            .pipeline(ImmutablePipelineParameters.builder().flavour(Configuration.Flavour.ADAM).build());

    public static final Configuration HUNDREDK_READS_HISEQ = DEFAULT_CONFIG_BUILDER.patient(DEFAULT_PATIENT_BUILDER.directory(
            System.getProperty("user.dir") + PATIENT_DIR + "/100k_reads_hiseq").name(HUNDREDK_READS_HISEQ_PATIENT_NAME).build()).build();
}
