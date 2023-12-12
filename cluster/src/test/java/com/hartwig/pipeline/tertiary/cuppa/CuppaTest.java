package com.hartwig.pipeline.tertiary.cuppa;

import static java.lang.String.format;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;
import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.linxSomaticOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.testsupport.TestInputs.virusInterpreterOutput;
import static com.hartwig.pipeline.tools.HmfTool.CUPPA;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class CuppaTest extends TertiaryStageTest<CuppaOutput> {

    private static final String TUMOR_CUPPA_CONCLUSION_TXT = "tumor.cuppa.conclusion.txt";
    private static final String TUMOR_CUP_REPORT_FEATURE_PNG = "tumor.cup.report.features.png";
    private static final String TUMOR_CUP_DATA_CSV = "tumor.cup.data.csv";
    private static final String TUMOR_CUP_REPORT_SUMMARY_PNG = "tumor.cup.report.summary.png";
    private static final String TUMOR_CUPPA_CHART_PNG = "tumor.cuppa.chart.png";
    private static final String TUMOR_CUP_REPORT = "tumor_cup_report.pdf";

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
        return List.of(new AddDatatype(DataType.CUPPA_SUMMARY_CHART,
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
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUP_REPORT_FEATURE_PNG)),
                new AddDatatype(DataType.CUPPA_CHART,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUPPA_CHART_PNG)),
                new AddDatatype(DataType.CUPPA_REPORT,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUP_REPORT)));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.CUPPA_CONCLUSION, "cuppa/" + TUMOR_CUPPA_CONCLUSION_TXT);
        persistedDataset.addPath(DataType.CUPPA_SUMMARY_CHART, "cuppa/" + TUMOR_CUP_REPORT_SUMMARY_PNG);
        persistedDataset.addPath(DataType.CUPPA_RESULTS, "cuppa/" + TUMOR_CUP_DATA_CSV);
        persistedDataset.addPath(DataType.CUPPA_FEATURE_PLOT, "cuppa/" + TUMOR_CUP_REPORT_FEATURE_PNG);
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
                "cuppa/" + TUMOR_CUP_REPORT_FEATURE_PNG));
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
                "set/cuppa/" + TUMOR_CUP_REPORT_FEATURE_PNG));
    }

    @Override
    protected Stage<CuppaOutput, SomaticRunMetadata> createVictim() {
        return new Cuppa(purpleOutput(),
                linxSomaticOutput(),
                virusInterpreterOutput(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES,
                persistedDataset);
    }

    @Override
    protected List<String> expectedCommands() {
        // @formatter:off
        String cuppaCharPath = format("%s/%s/%s", VmDirectories.TOOLS, "cuppa-chart", CUPPA.runVersion());

        return List.of(toolCommand(CUPPA)
                + " -categories DNA "
                + "-ref_data_dir /opt/resources/cuppa/ "
                + "-ref_genome_version V37 "
                + "-sample tumor "
                + "-sample_data_dir /data/input/results "
                + "-output_dir /data/output "
                + "-create_pdf",
                cuppaCharPath + "_venv/bin/python " + cuppaCharPath + "/cuppa-chart.py "
                + "-sample tumor "
                + "-sample_data /data/output/" + TUMOR_CUP_DATA_CSV
                + " -output_dir /data/output");
        // @formatter:on
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
                ResultsDirectory.defaultDirectory().path(TUMOR_CUP_REPORT_FEATURE_PNG)));
    }

    @Override
    protected boolean isEnabledOnShallowSeq() {
        return false;
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(expectedRuntimeBucketName() + "/purple/results/", "results"),
                input(expectedRuntimeBucketName() + "/linx/results/", "results"),
                input(expectedRuntimeBucketName() + "/virusintrprtr/tumor.virus.annotated.tsv", "tumor.virus.annotated.tsv"));
    }
}