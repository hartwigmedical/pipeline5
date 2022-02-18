package com.hartwig.pipeline.tertiary.purple;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.reruns.NoopPersistedDataset;
import com.hartwig.pipeline.resource.RefGenome37ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class PurpleTest extends TertiaryStageTest<PurpleOutput> {

    public static final String TUMOR_PURPLE_SOMATIC_VCF_GZ = "tumor.purple.somatic.vcf.gz";
    public static final String TUMOR_PURPLE_GERMLINE_VCF_GZ = "tumor.purple.germline.vcf.gz";
    public static final String TUMOR_PURPLE_SV_VCF_GZ = "tumor.purple.sv.vcf.gz";
    public static final String TUMOR_PURITY_TSV = "tumor.purple.purity.tsv";
    public static final String TUMOR_QC = "tumor.purple.qc";
    public static final String TUMOR_SOMATIC_DRIVER_CATALOG = "tumor.driver.catalog.somatic.tsv";
    public static final String TUMOR_GERMLINE_DRIVER_CATALOG = "tumor.driver.catalog.germline.tsv";
    public static final String TUMOR_SOMATIC_COPY_NUMBER = "tumor.purple.cnv.somatic.tsv";
    public static final String TUMOR_CIRCOS_PLOT = "plot/tumor.circos.png";
    public static final String TUMOR_GERMLINE_DRIVER_CNV = "tumor.purple.cnv.germline.tsv";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<PurpleOutput, SomaticRunMetadata> createVictim() {
        return new Purple(TestInputs.REF_GENOME_37_RESOURCE_FILES,
                TestInputs.paveSomaticOutput(),
                TestInputs.paveGermlineOutput(),
                TestInputs.gripssSomaticProcessOutput(),
                TestInputs.amberOutput(),
                TestInputs.cobaltOutput(),
                persistedDataset,
                false,
                true);
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/pave_somatic/results/tumor.somatic.vcf.gz", "tumor.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/pave_germline/results/tumor.germline.vcf.gz", "tumor.germline.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.filtered.vcf.gz", "tumor.gripss.filtered.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.filtered.vcf.gz.tbi", "tumor.gripss.filtered.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.full.vcf.gz", "tumor.gripss.full.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.full.vcf.gz.tbi", "tumor.gripss.full.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/amber/results/", "results"),
                input(expectedRuntimeBucketName() + "/cobalt/results/", "results"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx12G -jar /opt/tools/purple/3.3/purple.jar "
                + "-reference reference -germline_vcf /data/input/tumor.germline.vcf.gz "
                + "-germline_hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz "
                + "-germline_del_freq_file /opt/resources/purple/37/cohort_germline_del_freq.37.csv "
                + "-tumor tumor -output_dir /data/output -amber /data/input/results -cobalt /data/input/results "
                + "-gc_profile /opt/resources/gc_profiles/37/GC_profile.1000bp.37.cnp "
                + "-somatic_vcf /data/input/tumor.somatic.vcf.gz -structural_vcf /data/input/tumor.gripss.filtered.vcf.gz "
                + "-sv_recovery_vcf /data/input/tumor.gripss.full.vcf.gz -circos /opt/tools/circos/0.69.6/bin/circos "
                + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                + "-run_drivers -somatic_hotspots /opt/resources/sage/37/KnownHotspots.somatic.37.vcf.gz "
                + "-driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv "
                + "-threads $(grep -c '^processor' /proc/cpuinfo)");
    }

    @Test
    public void shallowModeUsesLowDepthSettings() {
        Purple victim = new Purple(new RefGenome37ResourceFiles(),
                TestInputs.paveSomaticOutput(),
                TestInputs.paveGermlineOutput(),
                TestInputs.gripssSomaticProcessOutput(),
                TestInputs.amberOutput(),
                TestInputs.cobaltOutput(),
                new NoopPersistedDataset(),
                true,
                true);
        assertThat(victim.commands(input()).get(0).asBash()).contains("-highly_diploid_percentage 0.88 -somatic_min_purity_spread 0.1");
    }

    @Override
    protected void validateOutput(final PurpleOutput output) {
        String bucketName = expectedRuntimeBucketName() + "/" + Purple.NAMESPACE;
        assertThat(output.outputLocations().outputDirectory().bucket()).isEqualTo(bucketName);
        assertThat(output.outputLocations().outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputLocations().outputDirectory().isDirectory()).isTrue();
        assertThat(output.outputLocations().somaticVcf().bucket()).isEqualTo(bucketName);
        assertThat(output.outputLocations().somaticVcf().path()).isEqualTo("results/" + TUMOR_PURPLE_SOMATIC_VCF_GZ);
        assertThat(output.outputLocations().somaticVcf().isDirectory()).isFalse();
        assertThat(output.outputLocations().structuralVcf().bucket()).isEqualTo(bucketName);
        assertThat(output.outputLocations().structuralVcf().path()).isEqualTo("results/" + TUMOR_PURPLE_SV_VCF_GZ);
        assertThat(output.outputLocations().structuralVcf().isDirectory()).isFalse();
    }

    @Override
    protected void validatePersistedOutput(final PurpleOutput output) {
        assertThat(output.outputLocations().outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/purple", true));
        assertThat(output.outputLocations().somaticVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/purple/" + TUMOR_PURPLE_SOMATIC_VCF_GZ));
        assertThat(output.outputLocations().structuralVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/purple/" + TUMOR_PURPLE_SV_VCF_GZ));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.STRUCTURAL_VARIANTS_PURPLE, "purple/" + TUMOR_PURPLE_SV_VCF_GZ);
        persistedDataset.addPath(DataType.SOMATIC_VARIANTS_PURPLE, "purple/" + TUMOR_PURPLE_SOMATIC_VCF_GZ);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final PurpleOutput output) {
        assertThat(output.outputLocations().outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "purple", true));
        assertThat(output.outputLocations().somaticVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "purple/" + TUMOR_PURPLE_SOMATIC_VCF_GZ));
        assertThat(output.outputLocations().structuralVcf()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "purple/" + TUMOR_PURPLE_SV_VCF_GZ));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.GERMLINE_VARIANTS_PURPLE,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_PURPLE_GERMLINE_VCF_GZ)),
                new AddDatatype(DataType.SOMATIC_VARIANTS_PURPLE,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_PURPLE_SOMATIC_VCF_GZ)),
                new AddDatatype(DataType.STRUCTURAL_VARIANTS_PURPLE,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_PURPLE_SV_VCF_GZ)),
                new AddDatatype(DataType.PURPLE_PURITY,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_PURITY_TSV)),
                new AddDatatype(DataType.PURPLE_SOMATIC_DRIVER_CATALOG,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_SOMATIC_DRIVER_CATALOG)),
                new AddDatatype(DataType.PURPLE_GERMLINE_DRIVER_CATALOG,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_GERMLINE_DRIVER_CATALOG)),
                new AddDatatype(DataType.PURPLE_GERMLINE_COPY_NUMBER,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_GERMLINE_DRIVER_CNV)),
                new AddDatatype(DataType.PURPLE_QC,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_QC)),
                new AddDatatype(DataType.PURPLE_CIRCOS_PLOT,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_CIRCOS_PLOT)),
                new AddDatatype(DataType.PURPLE_SOMATIC_COPY_NUMBER,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_SOMATIC_COPY_NUMBER)));
    }
}