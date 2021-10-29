package com.hartwig.pipeline.tertiary.virus;

import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.pipeline.ResultsDirectory;
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

public class VirusAnalysisTest extends TertiaryStageTest<VirusOutput> {

    private static final String TUMOR_VIRUS_ANNOTATED_TSV = "tumor.virus.annotated.tsv";
    private static final String TUMOR_VIRUSBREAKEND_VCF_SUMMARY_TSV = "tumor.virusbreakend.vcf.summary.tsv";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<VirusOutput, SomaticRunMetadata> createVictim() {
        return new VirusAnalysis(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.VIRUSBREAKEND_VARIANTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), VirusAnalysis.NAMESPACE, "tumor.virusbreakend.vcf")),
                new AddDatatype(DataType.VIRUSBREAKEND_SUMMARY,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), VirusAnalysis.NAMESPACE, TUMOR_VIRUSBREAKEND_VCF_SUMMARY_TSV)),
                new AddDatatype(DataType.VIRUS_INTERPRETATION,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), VirusAnalysis.NAMESPACE, TUMOR_VIRUS_ANNOTATED_TSV)));
    }

    @Override
    protected void validateOutput(final VirusOutput output) {
        assertThat(output.annotatedVirusFile()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/virusbreakend",
                ResultsDirectory.defaultDirectory().path(TUMOR_VIRUS_ANNOTATED_TSV)));
    }

    @Override
    protected void validatePersistedOutput(final VirusOutput output) {
        assertThat(output.annotatedVirusFile()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/virusbreakend/" + TUMOR_VIRUS_ANNOTATED_TSV));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.VIRUSBREAKEND_SUMMARY, "virusbreakend/" + TUMOR_VIRUSBREAKEND_VCF_SUMMARY_TSV);
        persistedDataset.addPath(DataType.VIRUS_INTERPRETATION, "virusbreakend/" + TUMOR_VIRUS_ANNOTATED_TSV);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final VirusOutput output) {
        assertThat(output.annotatedVirusFile()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "virusbreakend/" + TUMOR_VIRUS_ANNOTATED_TSV));
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("export PATH=\"${PATH}:/opt/tools/gridss/2.11.1\"",
                "export PATH=\"${PATH}:/opt/tools/repeatmasker/4.1.1\"",
                "export PATH=\"${PATH}:/opt/tools/kraken2/2.1.0\"",
                "export PATH=\"${PATH}:/opt/tools/samtools/1.10\"",
                "export PATH=\"${PATH}:/opt/tools/bcftools/1.9\"",
                "export PATH=\"${PATH}:/opt/tools/bwa/0.7.17\"",
                "/opt/tools/gridss/2.11.1/virusbreakend.sh " + "--output /data/output/tumor.virusbreakend.vcf "
                        + "--workingdir /data/output "
                        + "--reference /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "--db /opt/resources/virusbreakend_db --jar /opt/tools/gridss/2.11.1/gridss.jar --gridssargs \"--jvmheap 60G\" /data/input/tumor.bam",
                "java -Xmx2G -jar /opt/tools/virus-interpreter/1.0/virus-interpreter.jar -sample_id tumor "
                        + "-virus_breakend_tsv /data/output/tumor.virusbreakend.vcf.summary.tsv "
                        + "-taxonomy_db_tsv /opt/resources/virus_interpreter/taxonomy_db.tsv "
                        + "-virus_interpretation_tsv /opt/resources/virus_interpreter/virus_interpretation.tsv "
                        + "-virus_blacklist_tsv /opt/resources/virus_interpreter/virus_blacklist.tsv -output_dir /data/output");
    }
}