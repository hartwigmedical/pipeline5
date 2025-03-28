package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.calling.structural.SvCalling.ASSEMBLE_CLASS_PATH;
import static com.hartwig.pipeline.calling.structural.SvCalling.CALLER_CLASS_PATH;
import static com.hartwig.pipeline.calling.structural.SvCalling.DEPTH_ANNOTATOR_CLASS_PATH;
import static com.hartwig.pipeline.calling.structural.SvCalling.PREP_CLASS_PATH;
import static com.hartwig.pipeline.testsupport.TestInputs.REFERENCE_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.TUMOR_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.tools.HmfTool.ESVEE;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;

public class EsveeTest extends StageTest<EsveeOutput, SomaticRunMetadata> {
    private static final String TUMOR_ESVEE_UNFILTERED_VCF_GZ = "tumor.esvee.unfiltered.vcf.gz";
    private static final String ESVEE_DIR = "esvee/";

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void returnsExpectedOutput() {
        // not supported currently
    }

    @Override
    public void returnsExpectedFurtherOperations() {
        // not supported currently
    }

    @Override
    public void addsLogs() {
        // not supported currently
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.ESVEE_UNFILTERED_VCF, ESVEE_DIR + TUMOR_ESVEE_UNFILTERED_VCF_GZ);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final EsveeOutput output) {
        assertThat(output.unfilteredVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, ESVEE_DIR + TUMOR_ESVEE_UNFILTERED_VCF_GZ));
    }

    @Override
    protected void validatePersistedOutput(final EsveeOutput output) {
        assertThat(output.unfilteredVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/esvee/" + TUMOR_ESVEE_UNFILTERED_VCF_GZ));
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runTertiary(false).build();
    }

    @Override
    protected Stage<EsveeOutput, SomaticRunMetadata> createVictim() {
        return new Esvee(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(inputDownload(REFERENCE_BUCKET, "reference.bam"),
                inputDownload(REFERENCE_BUCKET, "reference.bam.bai"),
                inputDownload(TUMOR_BUCKET, "tumor.bam"),
                inputDownload(TUMOR_BUCKET, "tumor.bam.bai"));
    }

    @Override
    protected String expectedRuntimeBucketName() {
        return TestInputs.SOMATIC_BUCKET;
    }

    @Override
    protected List<String> expectedCommands() {
        List<String> expectedCommands = Lists.newArrayList();

        // @formatter:off
        expectedCommands.add(
                toolCommand(ESVEE, PREP_CLASS_PATH)
                        + " -sample tumor,reference"
                        + " -bam_file /data/input/tumor.bam,/data/input/reference.bam"
                        + " -blacklist_bed /opt/resources/sv/37/sv_prep_blacklist.37.bed"
                        + " -known_fusion_bed /opt/resources/sv/37/known_fusions.37.bedpe"
                        + " -bamtool /opt/tools/sambamba/0.6.8/sambamba"
                        + " -write_types \"JUNCTIONS;BAM;FRAGMENT_LENGTH_DIST\""
                        + " -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
                        + " -ref_genome_version V37"
                        + " -output_dir /data/output"
                        + " -threads $(grep -c '^processor' /proc/cpuinfo)"
        );

        expectedCommands.add(
                toolCommand(ESVEE, ASSEMBLE_CLASS_PATH)
                        + " -tumor tumor"
                        + " -tumor_bam /data/output/tumor.esvee.prep.bam"
                        + " -reference reference"
                        + " -reference_bam /data/output/reference.esvee.prep.bam"
                        + " -junction_file /data/output/tumor.esvee.prep.junction.tsv"
                        + " -write_types \"JUNC_ASSEMBLY;PHASED_ASSEMBLY;ALIGNMENT;BREAKEND;VCF\""
                        + " -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
                        + " -ref_genome_version V37"
                        + " -decoy_genome /opt/resources/sv/37/hg38_decoys.fa.img"
                        + " -output_dir /data/output"
                        + " -threads $(grep -c '^processor' /proc/cpuinfo)"
        );

        expectedCommands.add(
                toolCommand(ESVEE, DEPTH_ANNOTATOR_CLASS_PATH)
                        + " -sample tumor,reference"
                        + " -bam_file /data/input/tumor.bam,/data/input/reference.bam"
                        + " -input_vcf /data/output/tumor.esvee.raw.vcf.gz"
                        + " -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
                        + " -ref_genome_version V37"
                        + " -unmap_regions /opt/resources/mappability/37/unmap_regions.37.tsv"
                        + " -output_dir /data/output"
                        + " -threads $(grep -c '^processor' /proc/cpuinfo)"
        );

        expectedCommands.add(
                toolCommand(ESVEE, CALLER_CLASS_PATH)
                        + " -sample tumor"
                        + " -reference reference"
                        + " -input_vcf /data/output/tumor.esvee.ref_depth.vcf.gz"
                        + " -ref_genome_version V37"
                        + " -known_hotspot_file /opt/resources/sv/37/known_fusions.37.bedpe"
                        + " -pon_sgl_file /opt/resources/sv/37/sgl_pon.37.bed.gz"
                        + " -pon_sv_file /opt/resources/sv/37/sv_pon.37.bedpe.gz"
                        + " -repeat_mask_file /opt/resources/sv/37/repeat_mask_data.37.fa.gz"
                        + " -output_dir /data/output"
        );
        // @formatter:on

        return expectedCommands;
    }

    @Override
    protected void validateOutput(final EsveeOutput output) {
        // no further validation yet
    }

    private String inputDownload(final String bucket, final String basename) {
        return input(format("%s/aligner/results/%s", bucket, basename), basename);
    }
}
