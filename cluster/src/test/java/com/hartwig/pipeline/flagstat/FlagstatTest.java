package com.hartwig.pipeline.flagstat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class FlagstatTest extends StageTest<FlagstatOutput, SingleSampleRunMetadata> {

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaults();
    }

    @Override
    public void disabledAppropriately() {
        // cannot be disabled
    }

    @Override
    protected Stage<FlagstatOutput, SingleSampleRunMetadata> createVictim() {
        return new Flagstat(TestInputs.referenceAlignmentOutput());
    }

    @Override
    protected SingleSampleRunMetadata input() {
        return TestInputs.referenceRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input("run-reference-test/aligner/results/reference.bam", "reference.bam"));
    }

    @Override
    protected List<String> expectedResources() {
        return Collections.emptyList();
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-reference-test";
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "($TOOLS_DIR/sambamba/0.6.8/sambamba flagstat -t $(grep -c '^processor' /proc/cpuinfo) /data/input/reference.bam > "
                        + "/data/output/reference.flagstat)");
    }

    @Override
    protected void validateOutput(final FlagstatOutput output) {
        // no additional
    }
}