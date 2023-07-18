package com.hartwig.pipeline.tertiary.pave;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.ToolInfo.PAVE;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.stages.TestPersistedDataset;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class PaveSomaticTest extends StageTest<PaveOutput, SomaticRunMetadata> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<PaveOutput, SomaticRunMetadata> createVictim() {
        return new PaveSomatic(
                TestInputs.REF_GENOME_37_RESOURCE_FILES, TestInputs.sageSomaticOutput(), new TestPersistedDataset(), Arguments.testDefaults());
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                toolCommand(PAVE)
                        + " -sample tumor "
                        + "-vcf_file /data/input/tumor.somatic.vcf.gz "
                        + "-output_vcf_file /data/output/tumor.pave.somatic.vcf.gz "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-ref_genome_version V37 "
                        + "-driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv "
                        + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                        + "-mappability_bed /opt/resources/mappability/37/mappability_150.37.bed.gz "
                        + "-gnomad_freq_file /opt/resources/gnomad/37/gnomad_variants_v37.csv.gz "
                        + "-read_pass_only "
                        + "-pon_file /opt/resources/sage/37/SageGermlinePon.1000x.37.tsv.gz "
                        + "-pon_filters \"HOTSPOT:10:5;PANEL:6:5;UNKNOWN:6:0\"");
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input("run-reference-tumor-test/sage_somatic/results/tumor.somatic.vcf.gz", "tumor.somatic.vcf.gz"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return "run-reference-tumor-test";
    }

    @Override
    protected void validateOutput(final PaveOutput output) {
        // not supported currently
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.SOMATIC_VARIANTS_PAVE,
                TestInputs.tumorRunMetadata().barcode(),
                new ArchivePath(Folder.root(), PaveSomatic.NAMESPACE, "tumor.pave.somatic.vcf.gz")));
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runTertiary(false).build();
    }

    @Override
    protected void validatePersistedOutput(final PaveOutput output) {
        assertThat(output.annotatedVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/pave_somatic/tumor.pave.somatic.vcf.gz"));
    }
}