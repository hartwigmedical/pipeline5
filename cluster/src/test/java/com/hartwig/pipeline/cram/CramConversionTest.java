package com.hartwig.pipeline.cram;

import static java.lang.String.format;

import static com.hartwig.pipeline.resource.Hg37ResourceFiles.REFERENCE_GENOME_FASTA_HG37;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class CramConversionTest extends StageTest<CramOutput, SingleSampleRunMetadata> {
    private static String BUCKET_NAME = "run-reference-test";

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaults();
    }

    @Override
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(createDisabledArguments())).isFalse();
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(Arguments.testDefaultsBuilder().outputCram(true).build())).isTrue();
    }

    @Override
    protected Stage<CramOutput, SingleSampleRunMetadata> createVictim() {
        return new CramConversion(TestInputs.referenceAlignmentOutput());
    }

    @Override
    protected SingleSampleRunMetadata input() {
        return TestInputs.referenceRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(BUCKET_NAME + "/aligner/results/reference.bam", "reference.bam"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return BUCKET_NAME;
    }

    @Override
    protected List<String> expectedCommands() {
        String samtools = "/opt/tools/samtools/1.9/samtools";
        String input = "/data/input/reference.bam";
        String output = "/data/output/reference.cram";
        return ImmutableList.of(
                format("%s view -T %s -o %s -O cram,store_md=1,store_nm=1 -@ $(grep -c '^processor' /proc/cpuinfo) %s",
                        samtools, REFERENCE_GENOME_FASTA_HG37, output, input),
                format("%s index %s", samtools, output),
                format("java -Xmx4G -cp /opt/tools/bamcomp/1.0/bamcomp.jar com.hartwig.bamcomp.BamToCramValidator %s %s 6",
                        input, output));
    }

    @Override
    protected void validateOutput(CramOutput output) {
        // no additional validation
    }
}