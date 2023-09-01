package com.hartwig.pipeline.tertiary;

import static com.hartwig.pipeline.testsupport.TestInputs.defaultSomaticRunMetadata;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Test;

public abstract class TertiaryStageTest<S extends StageOutput> extends StageTest<S, SomaticRunMetadata> {

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runTertiary(false).build();
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input("run-tumor-test/aligner/results/tumor.bam", "tumor.bam"),
                input("run-tumor-test/aligner/results/tumor.bam.bai", "tumor.bam.bai"),
                input("run-reference-test/aligner/results/reference.bam", "reference.bam"),
                input("run-reference-test/aligner/results/reference.bam.bai", "reference.bam.bai"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-reference-tumor-test";
    }

    @Test
    public void persistedOutputDatatypesMatchOutputDatatypes() {
        SomaticRunMetadata metadata = defaultSomaticRunMetadata();
        assertThat(victim.output(input(), PipelineStatus.SUCCESS, runtimeBucket, ResultsDirectory.defaultDirectory())
                .datatypes()).isEqualTo(victim.persistedOutput(metadata).datatypes());
    }
}
