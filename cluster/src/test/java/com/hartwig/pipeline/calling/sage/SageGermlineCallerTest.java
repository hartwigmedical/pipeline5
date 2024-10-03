package com.hartwig.pipeline.calling.sage;

import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.SAGE;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.TestPersistedDataset;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class SageGermlineCallerTest extends TertiaryStageTest<SageOutput> {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected List<String> expectedInputs() {
        List<String> expectedInputs = Lists.newArrayList(super.expectedInputs());
        expectedInputs.add(input(TestInputs.TUMOR_BUCKET + "/aligner/results/tumor.jitter_params.tsv", "tumor.jitter_params.tsv"));
        expectedInputs.add(input(TestInputs.TUMOR_BUCKET + "/aligner/results/tumor.ms_table.tsv.gz", "tumor.ms_table.tsv.gz"));
        expectedInputs.add(input(TestInputs.REFERENCE_BUCKET + "/aligner/results/reference.jitter_params.tsv", "reference.jitter_params.tsv"));
        expectedInputs.add(input(TestInputs.REFERENCE_BUCKET + "/aligner/results/reference.ms_table.tsv.gz", "reference.ms_table.tsv.gz"));
        return expectedInputs;
    }

    @Override
    protected Stage<SageOutput, SomaticRunMetadata> createVictim() {
        return new SageGermlineCaller(TestInputs.defaultPair(),
                new TestPersistedDataset(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES);
    }

    @Override
    protected List<String> expectedCommands() {
        return ImmutableList.of(
                toolCommand(SAGE)
                        + " -tumor reference"
                        + " -tumor_bam /data/input/reference.bam"
                        + " -reference tumor -reference_bam /data/input/tumor.bam"
                        + " -hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz"
                        + " -germline"
                        + " -panel_only"
                        + " -ref_sample_count 0"
                        + " -jitter_param_dir /data/input/"
                        + " -high_confidence_bed /opt/resources/giab_high_conf/37/NA12878_GIAB_highconf_IllFB-IllGATKHC-CG-Ion-Solid_ALLCHROM_v3.2.2_highconf.bed.gz"
                        + " -panel_bed /opt/resources/sage/37/ActionableCodingPanel.37.bed.gz"
                        + " -coverage_bed /opt/resources/sage/37/CoverageCodingPanel.37.bed.gz"
                        + " -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
                        + " -ref_genome_version V37"
                        + " -ensembl_data_dir /opt/resources/ensembl_data_cache/37/"
                        + " -output_vcf /data/output/tumor.sage.germline.vcf.gz"
                        + " -bqr_write_plot"
                        + " -threads $(grep -c '^processor' /proc/cpuinfo)");
    }

    @Override
    public void returnsExpectedOutput() {
        // not supported currently
    }

    @Override
    protected void validateOutput(final SageOutput output) {
        // not supported currently
    }

    @Override
    public void addsLogs() {
        // not supported currently
    }

    @Override
    protected void validatePersistedOutput(final SageOutput output) {
        assertThat(output.variants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/sage_germline/tumor.sage.germline.vcf.gz"));
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // ignore for now
    }
}