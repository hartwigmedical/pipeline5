package com.hartwig.pipeline.tertiary.lilac;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

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
                "/opt/tools/samtools/1.20/samtools view -L /opt/resources/lilac/37/hla.37.bed -@ $(grep -c '^processor' /proc/cpuinfo) -u -M -T /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -o /data/output/reference.hla.bam /data/input/reference.bam",
                "/opt/tools/sambamba/0.6.8/sambamba index /data/output/reference.hla.bam",
                "/opt/tools/samtools/1.20/samtools view -L /opt/resources/lilac/37/hla.37.bed -@ $(grep -c '^processor' /proc/cpuinfo) -u -M -T /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -o /data/output/tumor.hla.bam /data/input/tumor.bam",
                "/opt/tools/sambamba/0.6.8/sambamba index /data/output/tumor.hla.bam");
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(TestInputs.TUMOR_BUCKET + "/aligner/results/tumor.bam", "tumor.bam"),
                input(TestInputs.TUMOR_BUCKET + "/aligner/results/tumor.bam.bai", "tumor.bam.bai"),
                input(TestInputs.REFERENCE_BUCKET + "/aligner/results/reference.bam", "reference.bam"),
                input(TestInputs.REFERENCE_BUCKET + "/aligner/results/reference.bam.bai", "reference.bam.bai"));
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
