package com.hartwig.pipeline.tertiary.purple;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.resource.RefGenome37ResourceFiles;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.stages.TestPersistedDataset;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.junit.Before;
import org.junit.Test;

public class PurpleTest extends TertiaryStageTest<PurpleOutput> {

    public static final String TUMOR_PURPLE_SOMATIC_VCF_GZ = "tumor.purple.somatic.vcf.gz";
    public static final String TUMOR_PURPLE_GERMLINE_VCF_GZ = "tumor.purple.germline.vcf.gz";
    public static final String TUMOR_PURPLE_SOMATIC_SV_VCF_GZ = "tumor.purple.sv.vcf.gz";
    public static final String TUMOR_PURPLE_GERMLINE_SV_VCF_GZ = "tumor.purple.sv.germline.vcf.gz";
    public static final String TUMOR_PURITY_TSV = "tumor.purple.purity.tsv";
    public static final String TUMOR_QC = "tumor.purple.qc";
    public static final String TUMOR_SOMATIC_DRIVER_CATALOG = "tumor.driver.catalog.somatic.tsv";
    public static final String TUMOR_GERMLINE_DRIVER_CATALOG = "tumor.driver.catalog.germline.tsv";
    public static final String TUMOR_SOMATIC_COPY_NUMBER = "tumor.purple.cnv.somatic.tsv";
    public static final String TUMOR_CIRCOS_PLOT = "plot/tumor.circos.png";
    public static final String TUMOR_GERMLINE_DELETION = "tumor.purple.germline.deletion.tsv";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<PurpleOutput, SomaticRunMetadata> createVictim() {
        return new Purple(TestInputs.REF_GENOME_37_RESOURCE_FILES,
                TestInputs.paveSomaticOutput(),
                TestInputs.paveGermlineOutput(),
                TestInputs.gripssSomaticOutput(),
                TestInputs.gripssGermlineOutput(),
                TestInputs.amberOutput(),
                TestInputs.cobaltOutput(),
                persistedDataset,
                Arguments.testDefaults());
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultSomaticRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/pave_somatic/results/tumor.somatic.vcf.gz", "tumor.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/pave_germline/results/tumor.germline.vcf.gz", "tumor.germline.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.filtered.somatic.vcf.gz", "tumor.gripss.filtered.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.filtered.somatic.vcf.gz.tbi",
                        "tumor.gripss.filtered.somatic.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.somatic.vcf.gz", "tumor.gripss.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.somatic.vcf.gz.tbi", "tumor.gripss.somatic.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/gripss_germline/results/tumor.gripss.filtered.germline.vcf.gz", "tumor.gripss.filtered.germline.vcf.gz"),
                input(expectedRuntimeBucketName() + "/amber/results/", "results"),
                input(expectedRuntimeBucketName() + "/cobalt/results/", "results"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "java -Xmx18G -jar /opt/tools/purple/3.8.1/purple.jar "
                        + "-amber /data/input/results "
                        + "-cobalt /data/input/results "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-ref_genome_version V37 "
                        + "-driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv "
                        + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                        + "-gc_profile /opt/resources/gc_profiles/37/GC_profile.1000bp.37.cnp "
                        + "-output_dir /data/output "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo) "
                        + "-tumor tumor "
                        + "-somatic_vcf /data/input/tumor.somatic.vcf.gz "
                        + "-somatic_sv_vcf /data/input/tumor.gripss.filtered.somatic.vcf.gz "
                        + "-sv_recovery_vcf /data/input/tumor.gripss.somatic.vcf.gz "
                        + "-somatic_hotspots /opt/resources/sage/37/KnownHotspots.somatic.37.vcf.gz "
                        + "-circos /opt/tools/circos/0.69.6/bin/circos "
                        + "-reference reference "
                        + "-germline_vcf /data/input/tumor.germline.vcf.gz "
                        + "-germline_sv_vcf /data/input/tumor.gripss.filtered.germline.vcf.gz "
                        + "-germline_hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz "
                        + "-germline_del_freq_file /opt/resources/purple/37/cohort_germline_del_freq.37.csv");
    }

    @Override
    protected List<String> expectedReferenceOnlyCommands() {
        return Collections.singletonList("java -Xmx18G -jar /opt/tools/purple/3.8.1/purple.jar "
                        + "-amber /data/input/results "
                        + "-cobalt /data/input/results "
                        + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta "
                        + "-ref_genome_version V37 -driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv "
                        + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                        + "-gc_profile /opt/resources/gc_profiles/37/GC_profile.1000bp.37.cnp " + "-output_dir /data/output "
                        + "-threads $(grep -c '^processor' /proc/cpuinfo) " + "-reference reference "
                        + "-germline_vcf /data/input/tumor.germline.vcf.gz "
                        + "-germline_sv_vcf /data/input/tumor.gripss.filtered.germline.vcf.gz "
                        + "-germline_hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz "
                        + "-germline_del_freq_file /opt/resources/purple/37/cohort_germline_del_freq.37.csv -no_charts");
    }

    @Test
    public void shallowModeUsesLowDepthSettings() {
        Purple victim = new Purple(new RefGenome37ResourceFiles(),
                TestInputs.paveSomaticOutput(),
                TestInputs.paveGermlineOutput(),
                TestInputs.gripssSomaticOutput(),
                TestInputs.gripssGermlineOutput(),
                TestInputs.amberOutput(),
                TestInputs.cobaltOutput(),
                new TestPersistedDataset(),
                Arguments.testDefaultsBuilder().shallow(true).build());
        assertThat(victim.tumorReferenceCommands(input()).get(0).asBash()).contains(
                "-highly_diploid_percentage 0.88 -somatic_min_purity_spread 0.1");
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Override
    protected void validateOutput(final PurpleOutput output) {
        String bucketName = expectedRuntimeBucketName() + "/" + Purple.NAMESPACE;
        assertThat(output.outputLocations().outputDirectory().bucket()).isEqualTo(bucketName);
        assertThat(output.outputLocations().outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputLocations().outputDirectory().isDirectory()).isTrue();
        assertThat(output.outputLocations().somaticVariants().get().bucket()).isEqualTo(bucketName);
        assertThat(output.outputLocations().somaticVariants().get().path()).isEqualTo("results/" + TUMOR_PURPLE_SOMATIC_VCF_GZ);
        assertThat(output.outputLocations().somaticVariants().get().isDirectory()).isFalse();
        assertThat(output.outputLocations().structuralVariants().get().bucket()).isEqualTo(bucketName);
        assertThat(output.outputLocations().structuralVariants().get().path()).isEqualTo("results/" + TUMOR_PURPLE_SOMATIC_SV_VCF_GZ);
        assertThat(output.outputLocations().structuralVariants().get().isDirectory()).isFalse();
    }

    @Override
    protected void validatePersistedOutput(final PurpleOutput output) {
        assertThat(output.outputLocations().outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/purple", true));
        assertThat(output.outputLocations().somaticVariants().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/purple/" + TUMOR_PURPLE_SOMATIC_VCF_GZ));
        assertThat(output.outputLocations().structuralVariants().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/purple/" + TUMOR_PURPLE_SOMATIC_SV_VCF_GZ));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.PURPLE_GERMLINE_VARIANTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_PURPLE_GERMLINE_VCF_GZ)),
                new AddDatatype(DataType.PURPLE_SOMATIC_VARIANTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_PURPLE_SOMATIC_VCF_GZ)),
                new AddDatatype(DataType.PURPLE_SOMATIC_STRUCTURAL_VARIANTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_PURPLE_SOMATIC_SV_VCF_GZ)),
                new AddDatatype(DataType.PURPLE_GERMLINE_STRUCTURAL_VARIANTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_PURPLE_GERMLINE_SV_VCF_GZ)),
                new AddDatatype(DataType.PURPLE_PURITY,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_PURITY_TSV)),
                new AddDatatype(DataType.PURPLE_SOMATIC_DRIVER_CATALOG,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_SOMATIC_DRIVER_CATALOG)),
                new AddDatatype(DataType.PURPLE_GERMLINE_DRIVER_CATALOG,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_GERMLINE_DRIVER_CATALOG)),
                new AddDatatype(DataType.PURPLE_GERMLINE_DELETION,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Purple.NAMESPACE, TUMOR_GERMLINE_DELETION)),
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