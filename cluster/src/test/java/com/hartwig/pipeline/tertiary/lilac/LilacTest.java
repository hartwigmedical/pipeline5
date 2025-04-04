package com.hartwig.pipeline.tertiary.lilac;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.LILAC;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

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

public class LilacTest extends TertiaryStageTest<LilacOutput> {

    public static final String TUMOR_LILAC_CSV = "tumor.lilac.tsv";
    public static final String TUMOR_LILAC_QC_CSV = "tumor.lilac.qc.tsv";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<LilacOutput, SomaticRunMetadata> createVictim() {
        return new Lilac(TestInputs.lilacBamSliceOutput(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES,
                TestInputs.purpleOutput(),
                persistedDataset);
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(TestInputs.SOMATIC_BUCKET + "/lilac_slicer/tumor.hla.bam", "tumor.hla.bam"),
                input(TestInputs.SOMATIC_BUCKET + "/lilac_slicer/tumor.hla.bam.bai", "tumor.hla.bam.bai"),
                input(TestInputs.SOMATIC_BUCKET + "/lilac_slicer/reference.hla.bam", "reference.hla.bam"),
                input(TestInputs.SOMATIC_BUCKET + "/lilac_slicer/reference.hla.bam.bai", "reference.hla.bam.bai"),
                input(TestInputs.SOMATIC_BUCKET + "/purple/tumor.purple.cnv.gene.tsv", "tumor.purple.cnv.gene.tsv"),
                input(TestInputs.SOMATIC_BUCKET + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"));
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of(
                toolCommand(LILAC) // "java -Xmx15G -jar /opt/tools/lilac/1.4.2/lilac.jar "
                + " -sample tumor -reference_bam /data/input/reference.hla.bam "
                + "-tumor_bam /data/input/tumor.hla.bam "
                + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -ref_genome_version V37 "
                + "-resource_dir /opt/resources/lilac/ "
                + "-output_dir /data/output "
                + "-threads $(grep -c '^processor' /proc/cpuinfo) "
                + "-gene_copy_number /data/input/tumor.purple.cnv.gene.tsv "
                + "-somatic_vcf /data/input/tumor.purple.somatic.vcf.gz");
    }

    @Override
    protected void validateOutput(final LilacOutput output) {
        assertThat(output.result()).contains(GoogleStorageLocation.of(TestInputs.SOMATIC_BUCKET + "/lilac", "results/" + TUMOR_LILAC_CSV));
        assertThat(output.qc()).contains(GoogleStorageLocation.of(TestInputs.SOMATIC_BUCKET + "/lilac", "results/" + TUMOR_LILAC_QC_CSV));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.LILAC_OUTPUT,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Lilac.NAMESPACE, TUMOR_LILAC_CSV)),
                new AddDatatype(DataType.LILAC_QC_METRICS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Lilac.NAMESPACE, TUMOR_LILAC_QC_CSV)));
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validatePersistedOutput(final LilacOutput output) {
        assertThat(output.result()).contains(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/lilac/" + TUMOR_LILAC_CSV));
        assertThat(output.qc()).contains(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/lilac/" + TUMOR_LILAC_QC_CSV));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.LILAC_OUTPUT, TUMOR_LILAC_CSV);
        persistedDataset.addPath(DataType.LILAC_QC_METRICS, TUMOR_LILAC_QC_CSV);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final LilacOutput output) {
        assertThat(output.result()).contains(GoogleStorageLocation.of(OUTPUT_BUCKET, TUMOR_LILAC_CSV));
        assertThat(output.qc()).contains(GoogleStorageLocation.of(OUTPUT_BUCKET, TUMOR_LILAC_QC_CSV));
    }
}