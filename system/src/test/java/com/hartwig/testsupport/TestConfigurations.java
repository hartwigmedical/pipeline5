package com.hartwig.testsupport;

import com.hartwig.pipeline.Configuration;
import com.hartwig.pipeline.ImmutableConfiguration;

public class TestConfigurations {
    public static final String PATIENT_DIR = "/src/test/resources/patients";

    public static final ImmutableConfiguration.Builder DEFAULT_CONFIG_BUILDER = Configuration.builder()
            .sparkMaster("local[1]")
            .referenceGenomePath(
                    System.getProperty("user.dir") + "/src/test/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa");

    public static final String HUNDREDK_READS_HISEQ_PATIENT_NAME = "TESTX";

    public static final Configuration HUNDREDK_READS_HISEQ =
            DEFAULT_CONFIG_BUILDER.patientDirectory(System.getProperty("user.dir") + PATIENT_DIR + "/100k_reads_hiseq")
                    .patientName(HUNDREDK_READS_HISEQ_PATIENT_NAME)
                    .build();

}
