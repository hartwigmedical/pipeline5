package com.hartwig.pipeline.tertiary.amber;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class AmberTest extends TertiaryStageTest<AmberOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<AmberOutput, SomaticRunMetadata> createVictim() {
        return new Amber(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        String basenameSnpcheck = TestInputs.REF_GENOME_37_RESOURCE_FILES.amberSnpcheck()
                .substring(TestInputs.REF_GENOME_37_RESOURCE_FILES.amberSnpcheck().lastIndexOf("/") + 1);
        return List.of(new AddDatatype(DataType.AMBER,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Amber.NAMESPACE, "tumor.amber.baf.tsv")),
                new AddDatatype(DataType.AMBER_SNPCHECK,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Amber.NAMESPACE, basenameSnpcheck)));
    }

    @Override
    protected void validateOutput(final AmberOutput output) {
        assertThat(output.outputDirectory().bucket()).isEqualTo("run-reference-tumor-test/amber");
        assertThat(output.outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputDirectory().isDirectory()).isTrue();
    }

    @Override
    protected void validatePersistedOutput(final AmberOutput output) {
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/amber", true));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.AMBER, "amber");
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final AmberOutput output) {
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "amber", true));
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("java -Xmx32G -jar /opt/tools/amber/3.6/amber.jar -ref_genome "
                + "/opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -loci /opt/resources/amber/37/GermlineHetPon.37.vcf.gz "
                + "-tumor tumor -tumor_bam /data/input/tumor.bam -reference reference -reference_bam /data/input/reference.bam "
                + "-output_dir /data/output", "cp /opt/resources/amber/37/Amber.snpcheck.37.vcf /data/output");
    }
}