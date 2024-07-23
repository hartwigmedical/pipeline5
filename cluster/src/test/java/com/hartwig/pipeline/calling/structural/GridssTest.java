package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.calling.structural.gridss.stage.SvCalling.ASSEMBLE_CLASS_PATH;
import static com.hartwig.pipeline.calling.structural.gridss.stage.SvCalling.CALLER_CLASS_PATH;
import static com.hartwig.pipeline.calling.structural.gridss.stage.SvCalling.DEPTH_ANNOTATOR_CLASS_PATH;
import static com.hartwig.pipeline.calling.structural.gridss.stage.SvCalling.SV_PREP_CLASS_PATH;
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
import com.hartwig.pipeline.calling.structural.gridss.Gridss;
import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.StageTest;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.tools.HmfTool;

import org.junit.Before;

public class GridssTest extends StageTest<GridssOutput, SomaticRunMetadata> {
    private static final String TUMOR_GRIDSS_UNFILTERED_VCF_GZ = "tumor.gridss.unfiltered.vcf.gz";
    private static final String GRIDSS = "gridss/";

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
        persistedDataset.addPath(DataType.STRUCTURAL_VARIANTS_GRIDSS, GRIDSS + TUMOR_GRIDSS_UNFILTERED_VCF_GZ);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final GridssOutput output) {
        assertThat(output.unfilteredVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, GRIDSS + TUMOR_GRIDSS_UNFILTERED_VCF_GZ));
    }

    @Override
    protected void validatePersistedOutput(final GridssOutput output) {
        assertThat(output.unfilteredVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/gridss/" + TUMOR_GRIDSS_UNFILTERED_VCF_GZ));
    }

    @Override
    protected Arguments createDisabledArguments() {
        return Arguments.testDefaultsBuilder().runTertiary(false).build();
    }

    @Override
    protected Stage<GridssOutput, SomaticRunMetadata> createVictim() {
        return new Gridss(TestInputs.defaultPair(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
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
                toolCommand(ESVEE, SV_PREP_CLASS_PATH)
                        + " -sample reference,tumor"
                        + " -bam_files /data/input/reference.bam,/data/input/tumor.bam"
                        + " -blacklist_bed /opt/resources/gridss/37/sv_prep_blacklist.37.bed"
                        + " -known_fusion_bed /opt/resources/fusions/37/known_fusions.37.bedpe"
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
                        + " -junction_files /data/output/tumor.esvee.prep.junctions.tsv"
                        + " -write_types \"JUNC_ASSEMBLY;ALIGNMENT;ALIGNMENT_DATA;BREAKEND;VCF\""
                        + " -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
                        + " -ref_genome_version V37"
                        + " -output_dir /data/output"
                        + " -threads $(grep -c '^processor' /proc/cpuinfo)"
        );

        expectedCommands.add(
                toolCommand(ESVEE, DEPTH_ANNOTATOR_CLASS_PATH)
                        + " -samples reference,tumor"
                        + " -bam_files /data/input/reference.bam,/data/input/tumor.bam"
                        + " -input_vcf /data/output/tumor.esvee.raw.vcf.gz"
                        + " -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
                        + " -ref_genome_version V37"
                        + " -output_dir /data/output"
                        + " -threads $(grep -c '^processor' /proc/cpuinfo)"
        );

        expectedCommands.add(
                toolCommand(ESVEE, CALLER_CLASS_PATH)
                        + " -sample tumor"
                        + " -reference reference"
                        + " -input_vcf /data/output/tumor.esvee.ref_depth.vcf.gz"
                        + " -ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta"
                        + " -ref_genome_version V37"
                        + " -output_dir /data/output"
                        + " -threads $(grep -c '^processor' /proc/cpuinfo)"
        );


        expectedCommands.add("java -Xmx8G -Dsamjdk.create_index=true "
                + "-Dsamjdk.use_async_io_read_samtools=true "
                + "-Dsamjdk"
                + ".use_async_io_write_samtools=true "
                + "-Dsamjdk.use_async_io_write_tribble=true "
                + "-Dsamjdk.buffer_size=4194304 "
                + "-cp /opt/tools/gridss/" + HmfTool.GRIDSS.runVersion() + "/gridss.jar "
                + "gridss.AnnotateInsertedSequence "
                + "REFERENCE_SEQUENCE=/opt/resources/virus_reference_genome/human_virus.fa "
                + "INPUT=/data/output/tumor.gridss.driver.vcf.gz " + "OUTPUT=/data/output/tumor.gridss.unfiltered.vcf.gz "
                + "ALIGNMENT=APPEND WORKER_THREADS=$(grep -c '^processor' /proc/cpuinfo)");
        // @formatter:on

        return expectedCommands;
    }

    @Override
    protected void validateOutput(final GridssOutput output) {
        // no further validation yet
    }

    private String inputDownload(final String bucket, final String basename) {
        return input(format("%s/aligner/results/%s", bucket, basename), basename);
    }
}
