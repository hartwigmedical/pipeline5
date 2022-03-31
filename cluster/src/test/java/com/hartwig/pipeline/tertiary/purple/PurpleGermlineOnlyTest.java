package com.hartwig.pipeline.tertiary.purple;

import static com.hartwig.pipeline.tertiary.purple.PurpleTest.TUMOR_PURITY_TSV;
import static com.hartwig.pipeline.tertiary.purple.PurpleTest.TUMOR_QC;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
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
import org.junit.Test;

public class PurpleGermlineOnlyTest extends TertiaryStageTest<PurpleOutput> {

    public static final String REFERENCE_PURPLE_GERMLINE_VCF_GZ = "reference.purple.germline.vcf.gz";
    public static final String REFERENCE_GERMLINE_DELETION = "reference.purple.germline.deletion.tsv";
    public static final String REFERENCE_GERMLINE_DRIVER_CATALOG = "reference.driver.catalog.germline.tsv";
    public static final String REFERENCE_PURITY_TSV = "reference.purple.purity.tsv";
    public static final String REFERENCE_QC = "reference.purple.qc";

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
                false);
    }

    @Override
    protected SomaticRunMetadata input() {
        return TestInputs.defaultGermlineRunMetadata();
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(
                input(expectedRuntimeBucketName() + "/pave_somatic/results/tumor.somatic.vcf.gz", "tumor.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/pave_germline/results/tumor.germline.vcf.gz", "tumor.germline.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.filtered.vcf.gz", "tumor.gripss.filtered.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.filtered.vcf.gz.tbi", "tumor.gripss.filtered.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.full.vcf.gz", "tumor.gripss.full.vcf.gz"),
                input(expectedRuntimeBucketName() + "/gripss_somatic/results/tumor.gripss.full.vcf.gz.tbi", "tumor.gripss.full.vcf.gz.tbi"),
                input(expectedRuntimeBucketName() + "/amber/results/", "results"),
                input(expectedRuntimeBucketName() + "/cobalt/results/", "results"));

        /*
        // should be
        return ImmutableList.of(
                input(expectedRuntimeBucketName() + "/pave_germline/results/reference.germline.vcf.gz", "reference.germline.vcf.gz"),
                input(expectedRuntimeBucketName() + "/amber/results/", "results"),
                input(expectedRuntimeBucketName() + "/cobalt/results/", "results"));
        */
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx12G -jar /opt/tools/purple/pilot/purple.jar "
                + "-reference reference "
                + "-germline_vcf /data/input/tumor.germline.vcf.gz "
                + "-germline_hotspots /opt/resources/sage/37/KnownHotspots.germline.37.vcf.gz "
                + "-germline_del_freq_file /opt/resources/purple/37/cohort_germline_del_freq.37.csv "
                + "-amber /data/input/results "
                + "-cobalt /data/input/results "
                + "-ref_genome /opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -ref_genome_version V37 "
                + "-run_drivers -driver_gene_panel /opt/resources/gene_panel/37/DriverGenePanel.37.tsv "
                + "-ensembl_data_dir /opt/resources/ensembl_data_cache/37/ "
                + "-gc_profile /opt/resources/gc_profiles/37/GC_profile.1000bp.37.cnp "
                + "-output_dir /data/output "
                + "-threads $(grep -c '^processor' /proc/cpuinfo) "
                + "-no_charts"
        );
    }

    @Override
    protected void validateOutput(final PurpleOutput output) {
        String bucketName = expectedRuntimeBucketName() + "/" + Purple.NAMESPACE;
        assertThat(output.outputLocations().outputDirectory().bucket()).isEqualTo(bucketName);
        assertThat(output.outputLocations().outputDirectory().path()).isEqualTo("results");
        assertThat(output.outputLocations().outputDirectory().isDirectory()).isTrue();
        assertThat(output.outputLocations().germlineVariants().get().bucket()).isEqualTo(bucketName);
        assertThat(output.outputLocations().germlineVariants().get().path()).isEqualTo("results/" + REFERENCE_PURPLE_GERMLINE_VCF_GZ);
        assertThat(output.outputLocations().germlineVariants().get().isDirectory()).isFalse();
    }

    @Override
    protected void validatePersistedOutput(final PurpleOutput output) {
        assertThat(output.outputLocations().outputDirectory()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/purple", true));
        assertThat(output.outputLocations().germlineVariants().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/purple/" + REFERENCE_PURPLE_GERMLINE_VCF_GZ));
        assertThat(output.outputLocations().germlineDeletions().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/purple/" + REFERENCE_GERMLINE_DELETION));
        assertThat(output.outputLocations().germlineDriverCatalog().get()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/purple/" + REFERENCE_GERMLINE_DRIVER_CATALOG));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        String barcode = input().barcode();
        return List.of(
                new AddDatatype(DataType.PURPLE_PURITY, barcode, new ArchivePath(Folder.root(), Purple.NAMESPACE, REFERENCE_PURITY_TSV)),
                new AddDatatype(DataType.PURPLE_QC, input().barcode(), new ArchivePath(Folder.root(), Purple.NAMESPACE, REFERENCE_QC)),
                new AddDatatype(
                        DataType.GERMLINE_VARIANTS_PURPLE, input().barcode(), new ArchivePath(Folder.root(), Purple.NAMESPACE,
                        REFERENCE_PURPLE_GERMLINE_VCF_GZ)),
                new AddDatatype(
                        DataType.PURPLE_GERMLINE_DRIVER_CATALOG, input().barcode(), new ArchivePath(Folder.root(), Purple.NAMESPACE,
                        REFERENCE_GERMLINE_DRIVER_CATALOG)),
                new AddDatatype(
                        DataType.PURPLE_GERMLINE_DELETION, input().barcode(), new ArchivePath(Folder.root(), Purple.NAMESPACE,
                        REFERENCE_GERMLINE_DELETION))
        );
    }
}