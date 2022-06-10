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

public class LilacTest extends TertiaryStageTest<LilacOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<LilacOutput, SomaticRunMetadata> createVictim() {
        return new Lilac(TestInputs.lilacBamSliceOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, TestInputs.purpleOutput());
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input("run-reference-tumor-test/lilac_bam_slicer/tumor.hla.bam", "tumor.hla.bam"),
                input("run-reference-tumor-test/lilac_bam_slicer/tumor.hla.bam.bai", "tumor.hla.bam.bai"),
                input("run-reference-tumor-test/lilac_bam_slicer/reference.hla.bam", "reference.hla.bam"),
                input("run-reference-tumor-test/lilac_bam_slicer/reference.hla.bam.bai", "reference.hla.bam.bai"),
                input("run-reference-tumor-test/purple/tumor.purple.cnv.gene.tsv", "tumor.purple.cnv.gene.tsv"),
                input("run-reference-tumor-test/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"));
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("java -Xmx15G -jar /opt/tools/lilac/1.1/lilac.jar " + "-sample tumor -reference_bam /data/input/reference.hla.bam "
                + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -ref_genome_version V37 "
                + "-resource_dir /opt/resources/lilac/ " + "-output_dir /data/output " + "-threads $(grep -c '^processor' /proc/cpuinfo) "
                + "-tumor_bam /data/input/tumor.hla.bam " + "-gene_copy_number_file /data/input/tumor.purple.cnv.gene.tsv "
                + "-somatic_variants_file /data/input/tumor.purple.somatic.vcf.gz");
    }

    @Override
    protected void validateOutput(final LilacOutput output) {
        // Lilac output is currently not used by any other tools
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.LILAC_OUTPUT,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Lilac.NAMESPACE, "tumor.lilac.csv")),
                new AddDatatype(DataType.LILAC_QC_METRICS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Lilac.NAMESPACE, "tumor.lilac.qc.csv")));
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validatePersistedOutput(final LilacOutput output) {
        // No extra validation as Lilac output isn't used anywhere
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final LilacOutput output) {
        // No extra validation as Lilac output isn't used anywhere
    }
}