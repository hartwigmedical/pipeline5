package hmf.testsupport;

import hmf.patient.Lane;
import hmf.patient.Sample;
import hmf.pipeline.Configuration;
import hmf.pipeline.ImmutableConfiguration;

public class TestPatients {
    private static final String PATIENT_DIR = "/src/test/resources/patients";

    private static final ImmutableConfiguration.Builder DEFAULT_CONFIG_BUILDER = Configuration.builder()
            .patientDirectory(System.getProperty("user.dir") + PATIENT_DIR)
            .referencePath(System.getProperty("user.dir") + "/src/test/resources/reference/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa");

    public static final String HUNDREDK_READS_HISEQ_PATIENT_NAME = "TESTX_H7YRLADXX_S1";
    private static final Lane HUNDREDK_READS_HISEQ_LANE_1 = Lane.of(PATIENT_DIR, HUNDREDK_READS_HISEQ_PATIENT_NAME, 1);
    private static final Lane HUNDREDK_READS_HISEQ_LANE_2 = Lane.of(PATIENT_DIR, HUNDREDK_READS_HISEQ_PATIENT_NAME, 2);
    public static final Sample HUNDREDK_READS_HISEQ_REAL_SAMPLE = Sample.builder(PATIENT_DIR, HUNDREDK_READS_HISEQ_PATIENT_NAME)
            .addLanes(HUNDREDK_READS_HISEQ_LANE_1, HUNDREDK_READS_HISEQ_LANE_2)
            .build();
    public static final Configuration HUNDREDK_READS_HISEQ =
            DEFAULT_CONFIG_BUILDER.patientName(HUNDREDK_READS_HISEQ_PATIENT_NAME).sparkMaster("local[1]").build();

}
