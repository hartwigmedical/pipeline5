package com.hartwig.pipeline.tertiary.bachelor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
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

public class BachelorTest extends TertiaryStageTest<BachelorOutput> {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<BachelorOutput, SomaticRunMetadata> createVictim() {
        return new Bachelor(persistedDataset,
                TestInputs.REF_GENOME_37_RESOURCE_FILES,
                TestInputs.purpleOutput(),
                TestInputs.tumorAlignmentOutput(),
                TestInputs.germlineCallerOutput());
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/purple/results/", "results"),
                input("run-tumor-test/aligner/results/tumor.bam", "tumor.bam"),
                input("run-tumor-test/aligner/results/tumor.bam.bai", "tumor.bam.bai"),
                input("run-reference-test/germline_caller/reference.germline.vcf.gz", "reference.germline.vcf.gz"),
                input("run-reference-test/germline_caller/reference.germline.vcf.gz.tbi", "reference.germline.vcf.gz.tbi"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList("java -Xmx8G -jar /opt/tools/bachelor/1.12/bachelor.jar -sample tumor -germline_vcf "
                + "/data/input/reference.germline.vcf.gz -tumor_bam_file /data/input/tumor.bam -purple_data_dir /data/input/results "
                + "-xml_config /opt/resources/bachelor_config/37/bachelor_hmf.xml -ext_filter_file "
                + "/opt/resources/bachelor_config/37/bachelor_clinvar_filters.csv -ref_genome "
                + "/opt/resources/reference_genome/37/Homo_sapiens.GRCh37.GATK.illumina.fasta -include_vcf_filtered -output_dir /data/output -log_debug");
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.BACHELOR,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Bachelor.NAMESPACE, "tumor.bachelor.germline_variant.tsv")),
                new AddDatatype(DataType.BACHELOR_REPORTABLE_VARIANTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Bachelor.NAMESPACE, "tumor.reportable_germline_variant.tsv")));
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final BachelorOutput output) {
        assertThat(output.reportableVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/bachelor/tumor.bachelor.germline_variant.tsv"));
    }

    @Override
    protected void validatePersistedOutput(final BachelorOutput output) {
        assertThat(output.reportableVariants()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/bachelor/tumor.bachelor.germline_variant.tsv"));
    }

    @Test
    public void doesntRunWhenGermlineDisabled() {
        assertThat(victim.shouldRun(Arguments.testDefaultsBuilder().runGermlineCaller(false).runTertiary(true).build())).isFalse();
    }

    @Override
    protected void validateOutput(final BachelorOutput output) {
        // no additional validation
    }

    @Override
    public void addsLogs() {
        // not supported currently
    }
}