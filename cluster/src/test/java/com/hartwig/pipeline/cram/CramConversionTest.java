package com.hartwig.pipeline.cram;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import java.util.List;

import static java.lang.String.format;

public class CramConversionTest extends StageTest<CramOutput, SingleSampleRunMetadata> {
    private static String BUCKET_NAME = "run-reference-test";

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaults();
    }

    @Override
    public void disabledAppropriately() {
        // cannot be disabled
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
                        samtools, Resource.REFERENCE_GENOME_FASTA, output, input),
                format("%s index %s", samtools, output),
                format("java -Xmx4G -cp /opt/tools/bamcomp/0.1/bamcomp.jar com.hartwig.bamcomp.BamToCramValidator %s %s 6",
                        input, output));
    }

    @Override
    protected void validateOutput(CramOutput output) {
        // no additional validation
    }
}