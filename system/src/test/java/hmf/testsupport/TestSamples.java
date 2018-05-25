package hmf.testsupport;

import hmf.pipeline.Configuration;
import hmf.pipeline.ImmutableConfiguration;
import hmf.sample.FlowCell;
import hmf.sample.Lane;
import hmf.sample.Sample;

public class TestSamples {
    private static final String SAMPLE_DIR = "/src/test/resources/samples";
    private static final String CANCER_PANEL_SAMPLE_NAME = "CPCT12345678R_HJJLGCCXX_S1_chr22";
    private static final Sample CANCER_PANEL_SAMPLE = Sample.of(SAMPLE_DIR, CANCER_PANEL_SAMPLE_NAME);
    public static final Lane CANCER_PANEL_LANE_1 = Lane.of(CANCER_PANEL_SAMPLE, 1);
    public static final Lane CANCER_PANEL_LANE_2 = Lane.of(CANCER_PANEL_SAMPLE, 2);
    private static final ImmutableConfiguration.Builder DEFAULT_CONFIG_BUILDER = Configuration.builder()
            .sampleDirectory(System.getProperty("user.dir") + SAMPLE_DIR)
            .referencePath(System.getProperty("user.dir") + "/src/test/resources/reference/Homo_sapiens.GRCh37.GATK.illumina.chr22.fa");
    public static final Configuration CANCER_PANEL = DEFAULT_CONFIG_BUILDER.sampleName(CANCER_PANEL_SAMPLE_NAME).build();

    private static final String HUNDREDK_READS_HISEQ_SAMPLE_NAME = "TESTX_H7YRLADXX_S1";
    private static final Sample HUNDREDK_READS_HISEQ_SAMPLE = Sample.of(SAMPLE_DIR, HUNDREDK_READS_HISEQ_SAMPLE_NAME);
    private static final Lane HUNDREDK_READS_HISEQ_LANE_1 = Lane.of(HUNDREDK_READS_HISEQ_SAMPLE, 1);
    private static final Lane HUNDREDK_READS_HISEQ_LANE_2 = Lane.of(HUNDREDK_READS_HISEQ_SAMPLE, 2);
    public static final FlowCell HUNDREDK_READS_HISEQ_FLOW_CELL =
            FlowCell.builder().sample(HUNDREDK_READS_HISEQ_SAMPLE).addLanes(HUNDREDK_READS_HISEQ_LANE_1, HUNDREDK_READS_HISEQ_LANE_2)
            .build();
    public static final Configuration HUNDREDK_READS_HISEQ = DEFAULT_CONFIG_BUILDER.sampleName(HUNDREDK_READS_HISEQ_SAMPLE_NAME).build();

}
