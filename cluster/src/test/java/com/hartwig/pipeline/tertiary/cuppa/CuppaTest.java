package com.hartwig.pipeline.tertiary.cuppa;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;
import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.linxOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

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

public class CuppaTest extends TertiaryStageTest<CuppaOutput> {

    private static final String TUMOR_CUPPA_CONCLUSION_TXT = "tumor.cuppa.conclusion.txt";
    private static final String TUMOR_CUPPA_CHART_PNG = "tumor.cuppa.chart.png";
    private static final String TUMOR_CUP_DATA_CSV = "tumor.cup.data.csv";
    private static final String TUMOR_CUP_REPORT_SUMMARY_PNG = "tumor.cup.report.summary.png";

    @Override
    public void disabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(false).build())).isFalse();
    }

    @Override
    public void enabledAppropriately() {
        assertThat(victim.shouldRun(testDefaultsBuilder().runTertiary(true).build())).isTrue();
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.CUPPA_CHART,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUP_REPORT_SUMMARY_PNG)),
                new AddDatatype(DataType.CUPPA_CONCLUSION,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUPPA_CONCLUSION_TXT)),
                new AddDatatype(DataType.CUPPA_RESULTS,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUP_DATA_CSV)),
                new AddDatatype(DataType.CUPPA_FEATURE_PLOT,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUPPA_CHART_PNG)));
    }

    @Override
    protected Stage<CuppaOutput, SomaticRunMetadata> createVictim() {
        return new Cuppa(purpleOutput(), linxOutput(), TestInputs.REF_GENOME_37_RESOURCE_FILES, persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of("java -Xmx4G -jar /opt/tools/cuppa/1.4/cuppa.jar -categories DNA -ref_data_dir /opt/resources/cuppa/ "
                        + "-sample_data tumor -sample_data_dir /data/input/results -sample_sv_file /data/input/tumor.purple.sv.vcf.gz "
                        + "-sample_somatic_vcf /data/input/tumor.purple.somatic.vcf.gz -output_dir /data/output",
                "/opt/tools/cuppa-chart/1.4_venv/bin/python /opt/tools/cuppa-chart/1.4/cuppa-chart.py -sample tumor "
                        + "-sample_data /data/output/" + TUMOR_CUP_DATA_CSV + " -output_dir /data/output",
                "Rscript /opt/tools/cuppa/1.4/CupGenerateReport_pipeline.R tumor /data/output/");
    }

    @Override
    protected void validateOutput(final CuppaOutput output) {
        assertThat(output.cuppaOutputLocations().conclusionTxt()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/cuppa",
                ResultsDirectory.defaultDirectory().path(TUMOR_CUPPA_CONCLUSION_TXT)));
        assertThat(output.cuppaOutputLocations().summaryChartPng()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/cuppa",
                ResultsDirectory.defaultDirectory().path(TUMOR_CUP_REPORT_SUMMARY_PNG)));
        assertThat(output.cuppaOutputLocations().resultCsv()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/cuppa",
                ResultsDirectory.defaultDirectory().path(TUMOR_CUP_DATA_CSV)));
        assertThat(output.cuppaOutputLocations().featurePlot()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/cuppa",
                ResultsDirectory.defaultDirectory().path(TUMOR_CUPPA_CHART_PNG)));
    }

    @Override
    protected void validatePersistedOutput(final CuppaOutput output) {
        assertThat(output.cuppaOutputLocations().conclusionTxt()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/cuppa/" + TUMOR_CUPPA_CONCLUSION_TXT));
        assertThat(output.cuppaOutputLocations().summaryChartPng()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/cuppa/" + TUMOR_CUP_REPORT_SUMMARY_PNG));
        assertThat(output.cuppaOutputLocations().resultCsv()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/cuppa/" + TUMOR_CUP_DATA_CSV));
        assertThat(output.cuppaOutputLocations().featurePlot()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/cuppa/" + TUMOR_CUPPA_CHART_PNG));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.CUPPA_CONCLUSION, "cuppa/" + TUMOR_CUPPA_CONCLUSION_TXT);
        persistedDataset.addPath(DataType.CUPPA_CHART, "cuppa/" + TUMOR_CUP_REPORT_SUMMARY_PNG);
        persistedDataset.addPath(DataType.CUPPA_RESULTS, "cuppa/" + TUMOR_CUP_DATA_CSV);
        persistedDataset.addPath(DataType.CUPPA_FEATURE_PLOT, "cuppa/" + TUMOR_CUPPA_CHART_PNG);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final CuppaOutput output) {
        assertThat(output.cuppaOutputLocations().conclusionTxt()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "cuppa/" + TUMOR_CUPPA_CONCLUSION_TXT));
        assertThat(output.cuppaOutputLocations().summaryChartPng()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "cuppa/" + TUMOR_CUP_REPORT_SUMMARY_PNG));
        assertThat(output.cuppaOutputLocations().resultCsv()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "cuppa/" + TUMOR_CUP_DATA_CSV));
        assertThat(output.cuppaOutputLocations().featurePlot()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "cuppa/" + TUMOR_CUPPA_CHART_PNG));
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.somatic.vcf.gz", "tumor.purple.somatic.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.sv.vcf.gz", "tumor.purple.sv.vcf.gz"),
                input(expectedRuntimeBucketName() + "/purple/results/", "results"),
                input(expectedRuntimeBucketName() + "/linx/results/", "results"));
    }
}