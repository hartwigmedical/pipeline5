package com.hartwig.pipeline.tertiary.purple;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatypeToFile;
import com.hartwig.pipeline.metadata.ApiFileOperation;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.reruns.NoopPersistedDataset;
import com.hartwig.pipeline.resource.Hg19ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class PurpleTest extends TertiaryStageTest<PurpleOutput> {

    public static final String TUMOR_PURPLE_SOMATIC_VCF_GZ = "tumor.purple.somatic.vcf.gz";
    public static final String TUMOR_PURPLE_SV_VCF_GZ = "tumor.purple.sv.vcf.gz";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<PurpleOutput, SomaticRunMetadata> createVictim() {
        return new Purple(TestInputs.HG19_RESOURCE_FILES,
                TestInputs.sageOutput(),
                TestInputs.structuralCallerPostProcessOutput(),
                TestInputs.amberOutput(),
                TestInputs.cobaltOutput(),
                persistedDataset,
                false);
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/sage/results/tumor.vcf.gz", "tumor.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss/results/tumor.gripss.filtered.vcf.gz", "tumor.gripss.filtered.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss/results/tumor.gripss.filtered.vcf.gz.tbi", "tumor.gripss.filtered.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/gripss/results/tumor.gripss.full.vcf.gz", "tumor.gripss.full.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss/results/tumor.gripss.full.vcf.gz.tbi", "tumor.gripss.full.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/amber/results/", "results"),
                input(expectedRuntimeBucketName() + "/cobalt/results/", "results"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "java -Xmx12G -jar /opt/tools/purple/2.48/purple.jar -reference reference -tumor tumor -output_dir "
                        + "/data/output -amber /data/input/results -cobalt /data/input/results -gc_profile /opt/resources/gc/hg19/GC_profile.1000bp.cnp "
                        + "-somatic_vcf /data/input/tumor.vcf.gz -structural_vcf /data/input/tumor.gripss.filtered.vcf.gz -sv_recovery_vcf "
                        + "/data/input/tumor.gripss.full.vcf.gz -circos /opt/tools/circos/0.69.6/bin/circos -ref_genome "
                        + "/opt/resources/reference_genome/hg19/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-driver_catalog -hotspots /opt/resources/sage/hg19/KnownHotspots.hg19.vcf.gz "
                        + "-driver_gene_panel /opt/resources/gene_panel/hg19/DriverGenePanel.hg19.tsv "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo)");
    }

    @Test
    public void shallowModeUsesLowDepthSettings() {
        Purple victim = new Purple(new Hg19ResourceFiles(),
                TestInputs.sageOutput(),
                TestInputs.structuralCallerPostProcessOutput(),
                TestInputs.amberOutput(),
                TestInputs.cobaltOutput(),
                new NoopPersistedDataset(),
                true);
        assertThat(victim.commands(input()).get(0).asBash()).contains(
                "-highly_diploid_percentage 0.88 -somatic_min_total 100 -somatic_min_purity_spread 0.1");
    }

    @Override
    protected void validateOutput(final PurpleOutput output) {
        String bucketName = expectedRuntimeBucketName() + "/" + Purple.NAMESPACE;
        assertThat(output.outputDirectory().bucket()).isEqualTo(bucketName);
        assertThat(output.outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputDirectory().isDirectory()).isTrue();
        assertThat(output.somaticVcf().bucket()).isEqualTo(bucketName);
        assertThat(output.somaticVcf().path()).isEqualTo("results/" + TUMOR_PURPLE_SOMATIC_VCF_GZ);
        assertThat(output.somaticVcf().isDirectory()).isFalse();
        assertThat(output.structuralVcf().bucket()).isEqualTo(bucketName);
        assertThat(output.structuralVcf().path()).isEqualTo("results/" + TUMOR_PURPLE_SV_VCF_GZ);
        assertThat(output.structuralVcf().isDirectory()).isFalse();
    }

    @Override
    protected void validatePersistedOutput(final PurpleOutput output) {
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/purple", true));
        assertThat(output.somaticVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/purple/" + TUMOR_PURPLE_SOMATIC_VCF_GZ));
        assertThat(output.structuralVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/purple/" + TUMOR_PURPLE_SV_VCF_GZ));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.STRUCTURAL_VARIANTS_PURPLE, "purple/" + TUMOR_PURPLE_SV_VCF_GZ);
        persistedDataset.addPath(DataType.SOMATIC_VARIANTS_PURPLE, "purple/" + TUMOR_PURPLE_SOMATIC_VCF_GZ);
        persistedDataset.addDir(DataType.SOMATIC_VARIANTS_PURPLE, "purple");
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final PurpleOutput output) {
        assertThat(output.outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "purple", true));
        assertThat(output.somaticVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "purple/" + TUMOR_PURPLE_SOMATIC_VCF_GZ));
        assertThat(output.structuralVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "purple/" + TUMOR_PURPLE_SV_VCF_GZ));
    }

    @Override
    protected List<ApiFileOperation> expectedFurtherOperations() {
        return List.of(new AddDatatypeToFile(DataType.SOMATIC_VARIANTS_PURPLE,
                        Folder.root(),
                        "purple",
                        TUMOR_PURPLE_SOMATIC_VCF_GZ,
                        TestInputs.defaultSomaticRunMetadata().barcode()),
                new AddDatatypeToFile(DataType.STRUCTURAL_VARIANTS_PURPLE,
                        Folder.root(),
                        "purple",
                        TUMOR_PURPLE_SV_VCF_GZ,
                        TestInputs.defaultSomaticRunMetadata().barcode()));
    }
}