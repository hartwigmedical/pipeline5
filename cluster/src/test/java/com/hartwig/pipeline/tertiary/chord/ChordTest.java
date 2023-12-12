package com.hartwig.pipeline.tertiary.chord;

import static java.lang.String.format;

import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;
import static com.hartwig.pipeline.tools.HmfTool.CHORD;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class ChordTest extends TertiaryStageTest<ChordOutput> {

    private static final String CHORD_PREDICTION_TXT = "tumor_chord_prediction.txt";

    @Override
    protected Stage<ChordOutput, SomaticRunMetadata> createVictim() {
        return new Chord(RefGenomeVersion.V37, TestInputs.purpleOutput(), persistedDataset);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.sv.vcf.gz", "tumor.purple.sv.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"));
    }

    @Override
    protected List<String> expectedCommands() {
        String chordPath = format("%s/%s/%s", VmDirectories.TOOLS, CHORD.getToolName(), CHORD.runVersion());
        return Collections.singletonList(chordPath + "/extractSigPredictHRD.R " + chordPath + " /data/output tumor "
                + "/data/input/tumor.purple.somatic.vcf.gz /data/input/tumor.purple.sv.vcf.gz HG37");
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final ChordOutput output) {
        assertThat(output.predictions()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/chord",
                ResultsDirectory.defaultDirectory().path(CHORD_PREDICTION_TXT)));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.CHORD_PREDICTION,
                TestInputs.defaultSomaticRunMetadata().barcode(),
                new ArchivePath(Folder.root(), Chord.NAMESPACE, CHORD_PREDICTION_TXT)));
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final ChordOutput output) {
        assertThat(output.predictions()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/chord/" + CHORD_PREDICTION_TXT));
    }

    @Override
    protected void validatePersistedOutput(final ChordOutput output) {
        assertThat(output.predictions()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/chord/" + CHORD_PREDICTION_TXT));
    }
}