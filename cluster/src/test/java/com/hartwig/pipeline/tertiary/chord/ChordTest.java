package com.hartwig.pipeline.tertiary.chord;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import java.util.List;

import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.CHORD;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class ChordTest extends TertiaryStageTest<ChordOutput> {

    private static final String CHORD_PREDICTION_TSV = "tumor.chord.prediction.tsv";
    private static final String CHORD_MUTATION_CONTEXTS_TSV = "tumor.chord.mutation_contexts.tsv";

    @Override
    protected Stage<ChordOutput, SomaticRunMetadata> createVictim() {
        return new Chord(TestInputs.purpleOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.sv.vcf.gz", "tumor.purple.sv.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"));
    }

    @Override
    protected List<String> expectedCommands() {
        String chordDir = format("%s/%s/%s", VmDirectories.TOOLS, CHORD.getToolName(), CHORD.runVersion());

        return List.of(
                toolCommand(CHORD)
                        + " -sample tumor"
                        + " -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
                        + " -purple_dir /data/input"
                        + " -output_dir /data/output"
                        + " -log_level DEBUG"
               );
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final ChordOutput output) {

        assertThat(output.chordOutputLocations().predictions()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/chord",
                ResultsDirectory.defaultDirectory().path(CHORD_PREDICTION_TSV)));

        assertThat(output.chordOutputLocations().signatures()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/chord",
                ResultsDirectory.defaultDirectory().path(CHORD_MUTATION_CONTEXTS_TSV)));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {

        return List.of(new AddDatatype(DataType.CHORD_PREDICTION,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Chord.NAMESPACE, CHORD_PREDICTION_TSV)),
                new AddDatatype(DataType.CHORD_MUTATION_CONTEXTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Chord.NAMESPACE, CHORD_MUTATION_CONTEXTS_TSV)));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.CHORD_PREDICTION, "chord/" + CHORD_PREDICTION_TSV);
        persistedDataset.addPath(DataType.CHORD_MUTATION_CONTEXTS, "chord/" + CHORD_MUTATION_CONTEXTS_TSV);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final ChordOutput output) {
        assertThat(output.chordOutputLocations().predictions()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "chord/" + CHORD_PREDICTION_TSV));
        assertThat(output.chordOutputLocations().signatures()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "chord/" + CHORD_MUTATION_CONTEXTS_TSV));
    }

    @Override
    protected void validatePersistedOutput(final ChordOutput output) {

        assertThat(output.chordOutputLocations().predictions()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/chord/" + CHORD_PREDICTION_TSV));
        assertThat(output.chordOutputLocations().signatures()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/chord/" + CHORD_MUTATION_CONTEXTS_TSV));
    }
}