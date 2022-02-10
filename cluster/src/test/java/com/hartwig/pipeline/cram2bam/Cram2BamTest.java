package com.hartwig.pipeline.cram2bam;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

public class Cram2BamTest extends StageTest<AlignmentOutput, SingleSampleRunMetadata> {

    @Override
    protected Arguments createDisabledArguments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void disabledAppropriately() {
        assertThat(true).isTrue();
    }

    @Override
    protected Stage<AlignmentOutput, SingleSampleRunMetadata> createVictim() {
        return new Cram2Bam(GoogleStorageLocation.of(TestInputs.BUCKET, FileTypes.bam(TestInputs.tumorSample())),
                SingleSampleRunMetadata.SampleType.TUMOR);
    }

    @Override
    protected SingleSampleRunMetadata input() {
        return TestInputs.tumorRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input("bucket/tumor.bam", "tumor.bam"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-tumor-test";
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("/opt/tools/samtools/1.14/samtools view -O bam -o /data/output/tumor.bam -@ $(grep -c '^processor' /proc/cpuinfo) "
                + "/data/input/tumor.bam", "/opt/tools/samtools/1.14/samtools index /data/output/tumor.bam");
    }

    @Override
    protected void validateOutput(final AlignmentOutput output) {
        assertThat(output.finalBamLocation()).isEqualTo(GoogleStorageLocation.of("run-tumor-test/cram2bam", "results/tumor.bam"));
        assertThat(output.finalBaiLocation()).isEqualTo(GoogleStorageLocation.of("run-tumor-test/cram2bam", "results/tumor.bam.bai"));
    }
}