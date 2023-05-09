package com.hartwig.pipeline.tertiary.chord;

import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
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
        return Collections.singletonList("/opt/tools/chord/2.02_1.14/extractSigPredictHRD.R /opt/tools/chord/2.02_1.14 /data/output tumor "
                + "/data/input/tumor.purple.somatic.vcf.gz /data/input/tumor.purple.sv.vcf.gz HG19");
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
        assertThat(output.predictions()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/chord/" + CHORD_PREDICTION_TXT));
    }

    @Override
    protected void validatePersistedOutput(final ChordOutput output) {
        assertThat(output.predictions()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/chord/" + CHORD_PREDICTION_TXT));
    }
}