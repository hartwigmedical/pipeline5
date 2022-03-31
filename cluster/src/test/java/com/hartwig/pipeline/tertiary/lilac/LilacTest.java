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
        return new Lilac(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, TestInputs.purpleOutput());
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input("run-tumor-test/aligner/results/tumor.bam", "tumor.bam"),
                input("run-tumor-test/aligner/results/tumor.bam.bai", "tumor.bam.bai"),
                input("run-reference-test/aligner/results/reference.bam", "reference.bam"),
                input("run-reference-test/aligner/results/reference.bam.bai", "reference.bam.bai"),
                input("run-reference-tumor-test/purple/tumor.purple.cnv.gene.tsv", "tumor.purple.cnv.gene.tsv"),
                input("run-reference-tumor-test/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"));
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of(
                "/opt/tools/sambamba/0.6.8/sambamba slice -L /opt/resources/lilac/37/hla.37.bed -o /data/output/reference.hla.bam /data/input/reference.bam",
                "/opt/tools/sambamba/0.6.8/sambamba index /data/output/reference.hla.bam",
                "/opt/tools/sambamba/0.6.8/sambamba slice -L /opt/resources/lilac/37/hla.37.bed -o /data/output/tumor.hla.bam /data/input/tumor.bam",
                "/opt/tools/sambamba/0.6.8/sambamba index /data/output/tumor.hla.bam",
                "java -Xmx15G -jar /opt/tools/lilac/1.1/lilac.jar "
                        + "-sample tumor -reference_bam /data/output/reference.hla.bam "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -ref_genome_version V37 "
                        + "-resource_dir /opt/resources/lilac/ "
                        + "-output_dir /data/output "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo) "
                        + "-tumor_bam /data/output/tumor.hla.bam "
                        + "-gene_copy_number_file /data/input/tumor.purple.cnv.gene.tsv "
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
}