package com.hartwig.pipeline.tertiary.lilac;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.resource.RefGenome37ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class LilacBamSlicerTest extends TertiaryStageTest<LilacBamSliceOutput> {
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<LilacBamSliceOutput, SomaticRunMetadata> createVictim() {
        return new LilacBamSlicer(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of(
                "/opt/tools/samtools/1.14/samtools view -f bam -L /opt/resources/lilac/37/hla.37.bed -b -o /data/output/reference.hla.bam /data/input/reference.bam",
                "/opt/tools/sambamba/0.6.8/sambamba index /data/output/reference.hla.bam",
                "/opt/tools/samtools/1.14/samtools view -f bam -L /opt/resources/lilac/37/hla.37.bed -b -o /data/output/tumor.hla.bam /data/input/tumor.bam",
                "/opt/tools/sambamba/0.6.8/sambamba index /data/output/tumor.hla.bam");
    }

    @Test
    public void usesSamtoolsCramSettingsWhenInputsAreCrams() {
        LilacBamSlicer victim = new LilacBamSlicer(AlignmentPair.of(AlignmentOutput.builder()
                        .sample(TestInputs.referenceSample())
                        .status(PipelineStatus.PERSISTED)
                        .maybeAlignments(GoogleStorageLocation.of("crams", TestInputs.referenceSample() + ".cram"))
                        .build(),
                AlignmentOutput.builder()
                        .sample(TestInputs.tumorSample())
                        .status(PipelineStatus.PERSISTED)
                        .maybeAlignments(GoogleStorageLocation.of("crams", TestInputs.tumorSample() + ".cram"))
                        .build()), new RefGenome37ResourceFiles(), persistedDataset);
        assertThat(victim.tumorReferenceCommands(input()).get(0).asBash()).isEqualTo("/opt/tools/samtools/1.14/samtools view -f cram -L /opt/resources/lilac/37/hla.37.bed -b -o /data/output/reference.hla.bam /data/input/reference.cram");
        assertThat(victim.tumorReferenceCommands(input()).get(2).asBash()).isEqualTo("/opt/tools/samtools/1.14/samtools view -f cram -L /opt/resources/lilac/37/hla.37.bed -b -o /data/output/tumor.hla.bam /data/input/tumor.cram");
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input("run-tumor-test/aligner/results/tumor.bam", "tumor.bam"),
                input("run-tumor-test/aligner/results/tumor.bam.bai", "tumor.bam.bai"),
                input("run-reference-test/aligner/results/reference.bam", "reference.bam"),
                input("run-reference-test/aligner/results/reference.bam.bai", "reference.bam.bai"));
    }

    @Override
    protected void validateOutput(final LilacBamSliceOutput output) {
        assertOutputFile(output.reference(), "reference.hla.bam");
        assertOutputFile(output.referenceIndex(), "reference.hla.bam.bai");
        assertOutputFile(output.tumor(), "tumor.hla.bam");
        assertOutputFile(output.tumorIndex(), "tumor.hla.bam.bai");
    }

    private void assertOutputFile(@SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<GoogleStorageLocation> maybeOutput,
            final String pathFromResults) {
        String outputDir = expectedRuntimeBucketName() + "/" + LilacBamSlicer.NAMESPACE;
        assertThat(maybeOutput.isPresent()).isTrue();
        GoogleStorageLocation reference = maybeOutput.get();
        assertThat(reference.bucket()).isEqualTo(outputDir);
        assertThat(reference.path()).isEqualTo("results/" + pathFromResults);
        assertThat(reference.isDirectory()).isFalse();

    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.LILAC_HLA_BAM,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LilacBamSlicer.NAMESPACE, "tumor.hla.bam")),
                new AddDatatype(DataType.LILAC_HLA_BAM_INDEX,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), LilacBamSlicer.NAMESPACE, "tumor.hla.bam.bai")),
                new AddDatatype(DataType.LILAC_HLA_BAM,
                        TestInputs.defaultSomaticRunMetadata().reference().barcode(),
                        new ArchivePath(Folder.root(), LilacBamSlicer.NAMESPACE, "reference.hla.bam")),
                new AddDatatype(DataType.LILAC_HLA_BAM_INDEX,
                        TestInputs.defaultSomaticRunMetadata().reference().barcode(),
                        new ArchivePath(Folder.root(), LilacBamSlicer.NAMESPACE, "reference.hla.bam.bai")));
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Override
    protected void validatePersistedOutput(final LilacBamSliceOutput output) {
        assertThat(output.reference().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/lilac_slicer/reference.hla.bam"));
        assertThat(output.referenceIndex().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/lilac_slicer/reference.hla.bam.bai"));
        assertThat(output.tumor().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/lilac_slicer/tumor.hla.bam"));
        assertThat(output.tumorIndex().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/lilac_slicer/tumor.hla.bam.bai"));
    }

    @Override
    protected void setupPersistedDataset() {
        // Tumor and reference share a datatype so we can only assert one or the other in this test
        persistedDataset.addPath(DataType.LILAC_HLA_BAM, "lilac_slicer/tumor.hla.bam");
        persistedDataset.addPath(DataType.LILAC_HLA_BAM_INDEX, "lilac_slicer/tumor.hla.bam.bai");
    }

    @Override
    public void validatePersistedOutputFromPersistedDataset(final LilacBamSliceOutput output) {
        assertThat(output.tumor().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "lilac_slicer/tumor.hla.bam"));
        assertThat(output.tumorIndex().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "lilac_slicer/tumor.hla.bam.bai"));
    }
}
