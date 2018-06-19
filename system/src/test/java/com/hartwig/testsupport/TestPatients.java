package com.hartwig.testsupport;

import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Configuration;
import com.hartwig.pipeline.ImmutableConfiguration;

public class TestPatients {
    public static final String PATIENT_DIR = "/src/test/resources/patients";

    public static final ImmutableConfiguration.Builder DEFAULT_CONFIG_BUILDER = Configuration.builder()
            .sparkMaster("local[1]")
            .referenceGenomePath(
                    System.getProperty("user.dir") + "/src/test/resources/reference_genome/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa");

    public static final String HUNDREDK_READS_HISEQ_PATIENT_NAME = "TESTX";
    private static final Lane HUNDREDK_READS_HISEQ_LANE_1 =
            Lane.of(PATIENT_DIR, "L001", HUNDREDK_READS_HISEQ_PATIENT_NAME + "R1.fastq", HUNDREDK_READS_HISEQ_PATIENT_NAME + "R2.fastq");
    private static final Lane HUNDREDK_READS_HISEQ_LANE_2 =
            Lane.of(PATIENT_DIR, "L002", HUNDREDK_READS_HISEQ_PATIENT_NAME + "R1.fastq", HUNDREDK_READS_HISEQ_PATIENT_NAME + "R2.fastq");
    public static final Sample HUNDREDK_READS_HISEQ_REAL_SAMPLE = Sample.builder(PATIENT_DIR, HUNDREDK_READS_HISEQ_PATIENT_NAME)
            .addLanes(HUNDREDK_READS_HISEQ_LANE_1, HUNDREDK_READS_HISEQ_LANE_2)
            .build();
    public static final Configuration HUNDREDK_READS_HISEQ =
            DEFAULT_CONFIG_BUILDER.patientDirectory(System.getProperty("user.dir") + PATIENT_DIR + "/100k_reads_hiseq")
                    .patientName(HUNDREDK_READS_HISEQ_PATIENT_NAME)
                    .build();

}
