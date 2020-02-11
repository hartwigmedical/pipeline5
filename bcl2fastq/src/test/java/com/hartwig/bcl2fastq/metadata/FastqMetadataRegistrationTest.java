package com.hartwig.bcl2fastq.metadata;

import static com.google.common.collect.Lists.newArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hartwig.bcl2fastq.conversion.Conversion;
import com.hartwig.bcl2fastq.conversion.ConvertedFastq;
import com.hartwig.bcl2fastq.conversion.ConvertedSample;
import com.hartwig.bcl2fastq.conversion.ConvertedUndetermined;
import com.hartwig.bcl2fastq.conversion.FastqId;
import com.hartwig.bcl2fastq.conversion.ImmutableConversion;
import com.hartwig.bcl2fastq.conversion.ImmutableConvertedFastq;
import com.hartwig.bcl2fastq.conversion.ImmutableConvertedSample;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class FastqMetadataRegistrationTest {

    private static final String EXISTS = "exists";
    private static final String TIMESTAMP = "2020-01-01T12:00:00";
    private static final String NEW_TIMESTAMP = "2020-01-02T12:00:00";
    private static final String BARCODE = "barcode";
    private static final String PROJECT = "project";
    private static final int Q30_REQ = 10;
    private static final int YLD_REQ = 2_000_000_000;
    private static final int FLOWCELL_DB_ID = 1;
    private static final int SAMPLE_ID = 2;
    private static final int LANE_ID = 3;
    public static final String OUTPUT_BUCKET = "output_bucket";
    public static final String OUTPUT_1 = "/output/1";
    public static final String OUTPUT_2 = "/output/2";
    public static final String SAMPLE_NAME = "sample";
    private SbpFastqMetadataApi sbpApi;
    private FastqMetadataRegistration victim;
    private ArgumentCaptor<SbpFlowcell> flowCellUpdateCaptor;
    private ArgumentCaptor<SbpSample> sampleUpdateCaptor;
    private ArgumentCaptor<SbpFastq> sbpFastqArgumentCaptor;
    public static final SbpSample SBP_SAMPLE = SbpSample.builder()
            .id(SAMPLE_ID)
            .q30_req(Q30_REQ)
            .yld_req(YLD_REQ)
            .barcode(BARCODE)
            .status("Waiting")
            .submission(PROJECT)
            .build();

    @Before
    public void setUp() {
        sbpApi = mock(SbpFastqMetadataApi.class);
        victim = new FastqMetadataRegistration(sbpApi, OUTPUT_BUCKET, "with 0 errors and");
        when(sbpApi.getFlowcell(EXISTS)).thenReturn(SbpFlowcell.builder()
                .id(1)
                .name(EXISTS)
                .flowcell_id(EXISTS)
                .status("Ready")
                .undet_rds_p_pass(true)
                .convertTime(TIMESTAMP)
                .updateTime(TIMESTAMP)
                .build());
        flowCellUpdateCaptor = ArgumentCaptor.forClass(SbpFlowcell.class);
        when(sbpApi.updateFlowcell(flowCellUpdateCaptor.capture())).thenReturn(SbpFlowcell.builder()
                .id(FLOWCELL_DB_ID)
                .name(EXISTS)
                .flowcell_id(EXISTS)
                .status("Ready")
                .undet_rds_p_pass(true)
                .convertTime(TIMESTAMP)
                .updateTime(NEW_TIMESTAMP)
                .build());
        when(sbpApi.findOrCreate(BARCODE, PROJECT)).thenReturn(SBP_SAMPLE);
        sampleUpdateCaptor = ArgumentCaptor.forClass(SbpSample.class);
        sbpFastqArgumentCaptor = ArgumentCaptor.forClass(SbpFastq.class);
        final SbpLane sbpLane = SbpLane.builder().flowcell_id(FLOWCELL_DB_ID).name("L001").build();
        when(sbpApi.findOrCreate(sbpLane)).thenReturn(SbpLane.builder().from(sbpLane).id(LANE_ID).build());
    }

    @Test(expected = IllegalStateException.class)
    public void noFlowcellFoundInSBPThrowsIllegalState() {
        victim.accept(conversion("no_exist").build());
    }

    @NotNull
    public ImmutableConversion.Builder conversion(final String name) {
        return Conversion.builder().undetermined(ConvertedUndetermined.builder().yield(0).yieldQ30(0).build()).flowcell(name);
    }

    @Test
    public void errorsInLogsFailsFlowcellQC() {
        victim = new FastqMetadataRegistration(sbpApi, "output_bucket", "with 1 errors and");
        victim.accept(conversion(EXISTS).build());
        assertThat(flowCellUpdateCaptor.getValue().undet_rds_p_pass()).isFalse();
    }

    @Test
    public void highUndeterminedYieldPercentageFailsFlowcellQC() {
        victim.accept(highUndeterminedReads());
        assertThat(flowCellUpdateCaptor.getValue().undet_rds_p_pass()).isFalse();
    }

    @Test
    public void anySamplesDontMeetMinYieldFailsFlowcellQC() {
        victim.accept(conversion(EXISTS).addSamples(sample().build(), sample().build()).build());
        assertThat(flowCellUpdateCaptor.getValue().undet_rds_p_pass()).isFalse();
    }

    @Test
    public void setsSampleStatusToReadyIfYieldAndQ30MeetsRequired() {
        when(sbpApi.getFastqs(SBP_SAMPLE)).thenReturn(newArrayList(sbpFastq(2_000_000_002, 100, true)));
        victim.accept(conversion(EXISTS).addSamples(sample().build()).build());
        verify(sbpApi).updateSample(sampleUpdateCaptor.capture());
        assertThat(sampleUpdateCaptor.getValue().status()).isEqualTo(SbpSample.STATUS_READY);
    }

    @Test
    public void setsNameOnSample() {
        victim.accept(conversion(EXISTS).addSamples(sample().build()).build());
        verify(sbpApi).updateSample(sampleUpdateCaptor.capture());
        assertThat(sampleUpdateCaptor.getValue().name()).hasValue(SAMPLE_NAME);
    }

    @Test
    public void sampleYieldCalculatedFromSampleFastq() {
        when(sbpApi.findOrCreate(BARCODE, PROJECT)).thenReturn(SBP_SAMPLE);
        when(sbpApi.getFastqs(SBP_SAMPLE)).thenReturn(newArrayList(sbpFastq(2_000_000_000L, 100, true),
                sbpFastq(2L, 100, true),
                sbpFastq(5L, 100, false)));
        victim.accept(conversion(EXISTS).addSamples(sample().build()).build());
        verify(sbpApi).updateSample(sampleUpdateCaptor.capture());
        SbpSample result = sampleUpdateCaptor.getValue();
        assertThat(result.yld()).hasValue(2_000_000_002L);
        assertThat(result.status()).isEqualTo(SbpSample.STATUS_READY);
    }

    @NotNull
    public ImmutableSbpFastq sbpFastq(long yield, double q30, boolean qcPass) {
        return SbpFastq.builder()
                .yld(yield)
                .q30(q30)
                .qc_pass(qcPass)
                .sample_id(1)
                .lane_id(1)
                .bucket("bucket")
                .name_r1("name_r1")
                .name_r2("name_r2")
                .build();
    }

    @Test
    public void existingSampleQ30ScaledAndAveragedWithNewFlowcell() {
        when(sbpApi.findOrCreate(BARCODE, PROJECT)).thenReturn(SBP_SAMPLE);
        when(sbpApi.getFastqs(SBP_SAMPLE)).thenReturn(newArrayList(sbpFastq(1000000000, 80, true), sbpFastq(2000000000, 90, true)));
        victim.accept(conversion(EXISTS).addSamples(sample().build()).build());
        verify(sbpApi).updateSample(sampleUpdateCaptor.capture());
        SbpSample result = sampleUpdateCaptor.getValue();
        assertThat(result.q30()).hasValue(86.66666666666667);
        assertThat(result.status()).isEqualTo(SbpSample.STATUS_READY);
    }

    @Test
    public void setsSampleStatusToInsufficientIfYieldLessThanRequired() {
        victim.accept(conversion(EXISTS).addSamples(sample().build()).build());
        verify(sbpApi).updateSample(sampleUpdateCaptor.capture());
        assertThat(sampleUpdateCaptor.getValue().status()).isEqualTo(SbpSample.STATUS_INSUFFICIENT_QUALITY);
    }

    @Test
    public void setsSampleStatusToReadyIfQ30LessThanRequired() {
        victim.accept(conversion(EXISTS).addSamples(sample().build()).build());
        verify(sbpApi).updateSample(sampleUpdateCaptor.capture());
        assertThat(sampleUpdateCaptor.getValue().status()).isEqualTo(SbpSample.STATUS_INSUFFICIENT_QUALITY);
    }

    @Test
    public void setsQcFailWhenFastQPairQ30LessThanRequired() {
        victim.accept(conversion(EXISTS).addSamples(sample().addFastq(fastq().yieldQ30(1).build()).build()).build());
        verify(sbpApi).create(sbpFastqArgumentCaptor.capture());
        assertThat(sbpFastqArgumentCaptor.getValue().qc_pass()).isFalse();
    }

    @Test
    public void setsQcFailOnFastQWhenFlowcellFails() {
        victim.accept(highUndeterminedReads());
        verify(sbpApi).create(sbpFastqArgumentCaptor.capture());
        assertThat(sbpFastqArgumentCaptor.getValue().qc_pass()).isFalse();
    }

    private ImmutableConversion highUndeterminedReads() {
        return conversion(EXISTS).undetermined(ConvertedUndetermined.builder().yieldQ30(7).yield(7).build())
                .addSamples(sample().addFastq(fastq().yield(100).build()).build())
                .build();
    }

    @Test
    public void createsFastQQCPassQ30MeetsRequired() {
        victim.accept(conversion(EXISTS).addSamples(sample().addFastq(fastq().build()).build()).build());
        verify(sbpApi).create(sbpFastqArgumentCaptor.capture());
        SbpFastq sbpFastq = sbpFastqArgumentCaptor.getValue();
        assertThat(sbpFastq.qc_pass()).isTrue();
        assertThat(sbpFastq.lane_id()).isEqualTo(LANE_ID);
        assertThat(sbpFastq.sample_id()).isEqualTo(SAMPLE_ID);
        assertThat(sbpFastq.bucket()).isEqualTo(OUTPUT_BUCKET);
        assertThat(sbpFastq.name_r1()).isEqualTo(OUTPUT_1);
        assertThat(sbpFastq.name_r2()).isEqualTo(OUTPUT_2);
        assertThat(sbpFastq.size_r1()).hasValue(1L);
        assertThat(sbpFastq.size_r2()).hasValue(2L);
        assertThat(sbpFastq.yld()).hasValue(1_000_000_000L);
        assertThat(sbpFastq.q30()).hasValue(90D);
        assertThat(sbpFastq.hash_r1()).hasValue("99de75");
        assertThat(sbpFastq.hash_r2()).hasValue("99de76");
    }

    @NotNull
    public ImmutableConvertedFastq.Builder fastq() {
        return ConvertedFastq.builder()
                .id(FastqId.of(1, BARCODE))
                .pathR1("/path/to/r1")
                .pathR2("/path/to/r2")
                .outputPathR1(OUTPUT_1)
                .outputPathR2(OUTPUT_2)
                .sizeR1(1)
                .sizeR2(2)
                .md5R1("md51")
                .md5R2("md52")
                .yieldQ30(900_000_000)
                .yield(1_000_000_000);
    }

    @NotNull
    public ImmutableConvertedSample.Builder sample() {
        return ConvertedSample.builder().barcode(BARCODE).project(PROJECT).sample(SAMPLE_NAME);
    }

    @Test
    public void flowcellUpdatedWithQCPassAndTimestamp() {
        victim.accept(conversion(EXISTS).build());
        assertThat(flowCellUpdateCaptor.getValue().undet_rds_p_pass()).isTrue();
        assertThat(flowCellUpdateCaptor.getAllValues().get(0).status()).isEqualTo(SbpFlowcell.STATUS_CONVERTED);
        assertThat(flowCellUpdateCaptor.getAllValues().get(1).convertTime()).hasValue(NEW_TIMESTAMP);
    }
}