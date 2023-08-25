package com.hartwig.pipeline.flagstat;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.input.SingleSampleRunMetadata;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FlagstatTest extends StageTest<FlagstatOutput, SingleSampleRunMetadata> {

    public static final String REFERENCE_FLAGSTAT = FlagstatOutput.outputFile(TestInputs.referenceSample());
    public static final AddDatatype ADD_DATATYPE = new AddDatatype(DataType.FLAGSTAT,
            TestInputs.referenceRunMetadata().barcode(),
            new ArchivePath(Folder.from(TestInputs.referenceRunMetadata()), Flagstat.NAMESPACE, "reference.flagstat"));

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
        return new Flagstat(TestInputs.referenceAlignmentOutput(), persistedDataset);
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
    protected String expectedRuntimeBucketName() {
        return "run-reference-test";
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "($TOOLS_DIR/samtools/1.14/samtools flagstat -@ $(grep -c '^processor' /proc/cpuinfo) /data/input/reference.bam > "
                        + "/data/output/reference.flagstat)");
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(ADD_DATATYPE);
    }

    @Override
    protected void validateOutput(final FlagstatOutput output) {
        // no additional
    }

    @Override
    protected void validatePersistedOutput(final FlagstatOutput output) {
        assertThat(output.flagstatOutputFile()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/reference/flagstat/" + REFERENCE_FLAGSTAT));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.FLAGSTAT, "flagstat/" + REFERENCE_FLAGSTAT);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final FlagstatOutput output) {
        assertThat(output.flagstatOutputFile()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "flagstat/" + REFERENCE_FLAGSTAT));
        assertThat(output.datatypes()).containsExactly(ADD_DATATYPE);
    }
}