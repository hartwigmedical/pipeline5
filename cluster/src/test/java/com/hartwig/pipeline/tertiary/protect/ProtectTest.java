package com.hartwig.pipeline.tertiary.protect;

import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class ProtectTest extends TertiaryStageTest<ProtectOutput> {

    private static final String TUMOR_PROTECT_TSV = "tumor.protect.tsv";

    @Override
    protected Stage<ProtectOutput, SomaticRunMetadata> createVictim() {
        return new Protect(TestInputs.purpleOutput(),
                TestInputs.linxOutput(),
                TestInputs.virusOutput(),
                TestInputs.chordOutput(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES,
                persistedDataset);
    }

    @Override
    protected List<String> expectedInputs() {
        return ImmutableList.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.purity.tsv", "tumor.purple.purity.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.qc", "tumor.purple.qc"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.cnv.gene.tsv", "tumor.purple.cnv.gene.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.driver.catalog.somatic.tsv", "tumor.driver.catalog.somatic.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.driver.catalog.germline.tsv", "tumor.driver.catalog.germline.tsv"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.germline.vcf.gz", "tumor.purple.germline.vcf.gz"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.fusion.tsv", "tumor.linx.fusion.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.breakend.tsv", "tumor.linx.breakend.tsv"),
                input(expectedRuntimeBucketName() + "/linx/tumor.linx.driver.catalog.tsv", "tumor.linx.driver.catalog.tsv"),
                input(expectedRuntimeBucketName() + "/virusbreakend/tumor.virus.annotated.tsv", "tumor.virus.annotated.tsv"),
                input(expectedRuntimeBucketName() + "/chord/tumor_chord_prediction.txt", "tumor_chord_prediction.txt"));
    }

    @Override
    protected List<String> expectedCommands() {
        return Collections.singletonList(
                "java -Xmx8G -jar /opt/tools/protect/1.4/protect.jar -tumor_sample_id tumor -reference_sample_id reference "
                        + "-primary_tumor_doids \"01;02\" -output_dir /data/output "
                        + "-serve_actionability_dir /opt/resources/serve/37/ -ref_genome_version 37 "
                        + "-doid_json /opt/resources/disease_ontology/201015_doid.json "
                        + "-purple_purity_tsv /data/input/tumor.purple.purity.tsv -purple_qc_file /data/input/tumor.purple.qc "
                        + "-purple_gene_copy_number_tsv /data/input/tumor.purple.cnv.gene.tsv "
                        + "-purple_somatic_driver_catalog_tsv /data/input/tumor.driver.catalog.somatic.tsv "
                        + "-purple_germline_driver_catalog_tsv /data/input/tumor.driver.catalog.germline.tsv "
                        + "-purple_somatic_variant_vcf /data/input/tumor.purple.somatic.vcf.gz "
                        + "-purple_germline_variant_vcf /data/input/tumor.purple.germline.vcf.gz "
                        + "-linx_fusion_tsv /data/input/tumor.linx.fusion.tsv -linx_breakend_tsv /data/input/tumor.linx.breakend.tsv "
                        + "-linx_driver_catalog_tsv /data/input/tumor.linx.driver.catalog.tsv "
                        + "-annotated_virus_tsv /data/input/tumor.virus.annotated.tsv "
                        + "-chord_prediction_txt /data/input/tumor_chord_prediction.txt");
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected void validateOutput(final ProtectOutput output) {
        assertThat(output.evidenceTsv()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/protect",
                ResultsDirectory.defaultDirectory().path(TUMOR_PROTECT_TSV)));
    }

    @Override
    protected void validatePersistedOutput(final ProtectOutput output) {
        assertThat(output.evidenceTsv()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "set/protect/" + TUMOR_PROTECT_TSV));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.PROTECT_EVIDENCE, "protect/" + TUMOR_PROTECT_TSV);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final ProtectOutput output) {
        assertThat(output.evidenceTsv()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "protect/" + TUMOR_PROTECT_TSV));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.PROTECT_EVIDENCE,
                TestInputs.defaultSomaticRunMetadata().barcode(),
                new ArchivePath(Folder.root(), Protect.NAMESPACE, "tumor.protect.tsv")));
    }
}