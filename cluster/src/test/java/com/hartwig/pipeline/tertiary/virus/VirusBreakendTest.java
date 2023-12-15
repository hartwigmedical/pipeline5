package com.hartwig.pipeline.tertiary.virus;

import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

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

import org.junit.Before;

public class VirusBreakendTest extends TertiaryStageTest<VirusBreakendOutput> {

    private static final String TUMOR_VIRUSBREAKEND_VCF_SUMMARY_TSV = "tumor.virusbreakend.vcf.summary.tsv";
    private static final String TUMOR_VIRUSBREAKEND_VCF = "tumor.virusbreakend.vcf";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<VirusBreakendOutput, SomaticRunMetadata> createVictim() {
        return new VirusBreakend(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedInputs() {
        return super.expectedInputs();
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.VIRUSBREAKEND_VARIANTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), VirusBreakend.NAMESPACE, "tumor.virusbreakend.vcf")),
                new AddDatatype(DataType.VIRUSBREAKEND_SUMMARY,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), VirusBreakend.NAMESPACE, TUMOR_VIRUSBREAKEND_VCF_SUMMARY_TSV)));
    }

    @Override
    protected void validateOutput(final VirusBreakendOutput output) {
        assertThat(output.maybeSummary()).isEqualTo(Optional.of(GoogleStorageLocation.of(SOMATIC_BUCKET + "/virusbreakend",
                ResultsDirectory.defaultDirectory().path(TUMOR_VIRUSBREAKEND_VCF_SUMMARY_TSV))));
        assertThat(output.maybeVariants()).isEqualTo(Optional.of(GoogleStorageLocation.of(SOMATIC_BUCKET + "/virusbreakend",
                ResultsDirectory.defaultDirectory().path(TUMOR_VIRUSBREAKEND_VCF))));
    }

    @Override
    protected void validatePersistedOutput(final VirusBreakendOutput output) {
        assertThat(output.summary()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/virusbreakend/" + TUMOR_VIRUSBREAKEND_VCF_SUMMARY_TSV));
        assertThat(output.variants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/virusbreakend/" + TUMOR_VIRUSBREAKEND_VCF));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.VIRUSBREAKEND_SUMMARY, "virusbreakend/" + TUMOR_VIRUSBREAKEND_VCF_SUMMARY_TSV);
        persistedDataset.addPath(DataType.VIRUSBREAKEND_VARIANTS, "virusbreakend/" + TUMOR_VIRUSBREAKEND_VCF);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final VirusBreakendOutput output) {
        assertThat(output.summary()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "virusbreakend/" + TUMOR_VIRUSBREAKEND_VCF_SUMMARY_TSV));
        assertThat(output.variants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "virusbreakend/" + TUMOR_VIRUSBREAKEND_VCF));
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("export PATH=\"${PATH}:/opt/tools/gridss/2.13.2\"",
                "export PATH=\"${PATH}:/opt/tools/repeatmasker/4.1.1\"",
                "export PATH=\"${PATH}:/opt/tools/kraken2/2.1.0\"",
                "export PATH=\"${PATH}:/opt/tools/samtools/1.14\"",
                "export PATH=\"${PATH}:/opt/tools/bcftools/1.9\"",
                "export PATH=\"${PATH}:/opt/tools/bwa/0.7.17\"",
                "/opt/tools/gridss/2.13.2/virusbreakend --output /data/output/tumor.virusbreakend.vcf " + "--workingdir /data/output "
                        + "--reference /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "--db /opt/resources/virusbreakend_db --jar /opt/tools/gridss/2.13.2/gridss.jar --gridssargs \"--jvmheap 60G\" "
                        + "/data/input/tumor.bam");
    }
}