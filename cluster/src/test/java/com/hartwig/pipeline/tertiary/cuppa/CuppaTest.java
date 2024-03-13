package com.hartwig.pipeline.tertiary.cuppa;

import static com.hartwig.pipeline.Arguments.testDefaultsBuilder;
import static com.hartwig.pipeline.tertiary.cuppa.Cuppa.CUPPA_DATA_PREP;
import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;
import static com.hartwig.pipeline.testsupport.TestInputs.linxSomaticOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.purpleOutput;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static com.hartwig.pipeline.testsupport.TestInputs.virusInterpreterOutput;
import static com.hartwig.pipeline.tools.HmfTool.CUPPA;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;

public class CuppaTest extends TertiaryStageTest<CuppaOutput> {

    private static final String TUMOR_CUPPA_VIS_DATA = "tumor.cuppa.vis_data.tsv";
    private static final String TUMOR_CUPPA_VIS_PLOT = "tumor.cuppa.vis.png";
    private static final String TUMOR_CUPPA_PRED_SUMM = "tumor.cuppa.pred_summ.tsv";

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
        return List.of(new AddDatatype(DataType.CUPPA_VIS_DATA,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUPPA_VIS_DATA)),
                new AddDatatype(DataType.CUPPA_VIS_PLOT,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUPPA_VIS_PLOT)),
                new AddDatatype(DataType.CUPPA_PRED_SUMM,
                        TestInputs.defaultSomaticRunMetadata().barcode(),
                        new ArchivePath(Folder.root(), Cuppa.NAMESPACE, TUMOR_CUPPA_PRED_SUMM)));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.CUPPA_VIS_DATA, "cuppa/" + TUMOR_CUPPA_VIS_DATA);
        persistedDataset.addPath(DataType.CUPPA_VIS_PLOT, "cuppa/" + TUMOR_CUPPA_VIS_PLOT);
        persistedDataset.addPath(DataType.CUPPA_PRED_SUMM, "cuppa/" + TUMOR_CUPPA_PRED_SUMM);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final CuppaOutput output) {
        assertThat(output.cuppaOutputLocations().visData()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "cuppa/" + TUMOR_CUPPA_VIS_DATA));
        assertThat(output.cuppaOutputLocations().visPlot()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "cuppa/" + TUMOR_CUPPA_VIS_PLOT));
        assertThat(output.cuppaOutputLocations().predSumm()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "cuppa/" + TUMOR_CUPPA_PRED_SUMM));
    }

    @Override
    protected void validatePersistedOutput(final CuppaOutput output) {
        assertThat(output.cuppaOutputLocations().visData()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/cuppa/" + TUMOR_CUPPA_VIS_DATA));
        assertThat(output.cuppaOutputLocations().visPlot()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/cuppa/" + TUMOR_CUPPA_VIS_PLOT));
        assertThat(output.cuppaOutputLocations().predSumm()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/cuppa/" + TUMOR_CUPPA_PRED_SUMM));
    }

    @Override
    protected Stage<CuppaOutput, SomaticRunMetadata> createVictim() {
        return new Cuppa(purpleOutput(),
                linxSomaticOutput(),
                virusInterpreterOutput(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES,
                persistedDataset, Arguments.testDefaults());
    }

    @Override
    protected List<String> expectedCommands() {
        // @formatter:off
        return List.of(toolCommand(CUPPA, CUPPA_DATA_PREP)
                + " -sample tumor "
                + "-categories DNA "
                + "-ref_genome_version V37 "
                + "-sample_data_dir /data/input/results "
                + "-output_dir /data/output",
                "(source /opt/tools/pycuppa/2.1.0rc_venv/bin/activate && "
                + "python -m cuppa.predict "
                + "--classifier_path /opt/resources/cuppa/37/cuppa_classifier.37.pickle.gz "
                + "--features_path /data/output/tumor.cuppa_data.tsv.gz --output_dir /data/output --sample_id tumor "
                + "&& deactivate)");
        // @formatter:on
    }

    @Override
    protected void validateOutput(final CuppaOutput output) {
        assertThat(output.cuppaOutputLocations().visData()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/cuppa",
                ResultsDirectory.defaultDirectory().path(TUMOR_CUPPA_VIS_DATA)));
        assertThat(output.cuppaOutputLocations().visPlot()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/cuppa",
                ResultsDirectory.defaultDirectory().path(TUMOR_CUPPA_VIS_PLOT)));
        assertThat(output.cuppaOutputLocations().predSumm()).isEqualTo(GoogleStorageLocation.of(SOMATIC_BUCKET + "/cuppa",
                ResultsDirectory.defaultDirectory().path(TUMOR_CUPPA_PRED_SUMM)));
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