package hmf.testsupport;

import hmf.pipeline.Configuration;
import hmf.sample.Lane;
import hmf.sample.Sample;

public class TestSamples {
    private static final String SAMPLE_NAME = "CPCT12345678R_HJJLGCCXX_S1_chr22";
    private static final String SAMPLE_DIR = "/src/test/resources/samples";
    public static final Configuration CONFIGURATION = Configuration.builder()
            .sampleDirectory(System.getProperty("user.dir") + SAMPLE_DIR)
            .sampleName(SAMPLE_NAME)
            .referencePath(System.getProperty("user.dir") + "/src/test/resources/reference/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa")
            .build();
    private static final Sample SAMPLE = Sample.of(SAMPLE_DIR, SAMPLE_NAME);
    public static final Lane LANE_1 = Lane.of(SAMPLE, 1);
    public static final Lane LANE_2 = Lane.of(SAMPLE, 2);
}
