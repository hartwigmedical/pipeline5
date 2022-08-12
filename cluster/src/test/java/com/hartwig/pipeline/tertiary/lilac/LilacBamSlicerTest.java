package com.hartwig.pipeline.tertiary.lilac;

import java.util.List;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
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
                "/opt/tools/sambamba/0.6.8/sambamba slice -L /opt/resources/lilac/37/hla.37.bed -o /data/output/reference.hla.bam /data/input/reference.bam",
                "/opt/tools/sambamba/0.6.8/sambamba index /data/output/reference.hla.bam",
                "/opt/tools/sambamba/0.6.8/sambamba slice -L /opt/resources/lilac/37/hla.37.bed -o /data/output/tumor.hla.bam /data/input/tumor.bam",
                "/opt/tools/sambamba/0.6.8/sambamba index /data/output/tumor.hla.bam");
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

    @Override
    protected void validatePersistedOutput(final LilacBamSliceOutput output) {

    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final LilacBamSliceOutput output) {

    }
}
