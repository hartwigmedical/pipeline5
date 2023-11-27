package com.hartwig.pipeline.tertiary.pave;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.PAVE;

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

public class PaveGermlineTest extends StageTest<PaveOutput, SomaticRunMetadata> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<PaveOutput, SomaticRunMetadata> createVictim() {
        return new PaveGermline(TestInputs.REF_GENOME_37_RESOURCE_FILES, TestInputs.sageGermlineOutput(), new TestPersistedDataset());
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                toolCommand(PAVE)
                        + " -sample tumor "
                        + "-vcf_file /data/input/tumor.germline.vcf.gz "
                        + "-output_vcf_file /data/output/tumor.pave.germline.vcf.gz "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-ref_genome_version V37 "
                        + "-driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv "
                        + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                        + "-mappability_bed /opt/resources/mappability/37/mappability_150.37.bed.gz "
                        + "-gnomad_freq_file /opt/resources/gnomad/37/gnomad_variants_v37.csv.gz "
                        + "-read_pass_only "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo) "
                        + "-clinvar_vcf /opt/resources/sage/37/clinvar.37.vcf.gz "
                        + "-blacklist_bed /opt/resources/sage/37/KnownBlacklist.germline.37.bed "
                        + "-blacklist_vcf /opt/resources/sage/37/KnownBlacklist.germline.37.vcf.gz "
                        + "-gnomad_pon_filter -1");
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(
                input("run-reference-tumor-test/sage_germline/results/tumor.germline.vcf.gz", "tumor.germline.vcf.gz"),
                input("run-reference-tumor-test/sage_germline/results/tumor.germline.vcf.gz.tbi", "tumor.germline.vcf.gz.tbi"));
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
        return List.of(new AddDatatype(DataType.GERMLINE_VARIANTS_PAVE,
                TestInputs.tumorRunMetadata().barcode(),
                new ArchivePath(Folder.root(), PaveGermline.NAMESPACE, "tumor.pave.germline.vcf.gz")));
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runTertiary(false).build();
    }

    @Override
    protected void validatePersistedOutput(final PaveOutput output) {
        assertThat(output.annotatedVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/pave_germline/tumor.pave.germline.vcf.gz"));
    }
}