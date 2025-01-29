package com.hartwig.pipeline.tertiary.virus;

import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.TertiaryStageTest;
import com.hartwig.pipeline.testsupport.TestInputs;
import com.hartwig.pipeline.tools.HmfTool;
import java.util.List;
import java.util.Optional;
import org.junit.Before;

import static com.hartwig.pipeline.metrics.BamMetrics.BAM_METRICS_SUMMARY_TSV;
import static com.hartwig.pipeline.testsupport.TestInputs.toolCommand;
import static org.assertj.core.api.Assertions.assertThat;

public class VirusInterpreterTest extends TertiaryStageTest<VirusInterpreterOutput> {

    private static final String ANNOTATED_VIRUS_TSV = "tumor.virus.annotated.tsv";

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected Stage<VirusInterpreterOutput, SomaticRunMetadata> createVictim() {
        return new VirusInterpreter(TestInputs.defaultPair(),
                TestInputs.REF_GENOME_37_RESOURCE_FILES,
                persistedDataset,
                TestInputs.virusBreakendOutput(),
                TestInputs.purpleOutput(),
                TestInputs.tumorMetricsOutput());
    }

    @Override
    protected List<String> expectedInputs() {
        return List.of(input(TestInputs.SOMATIC_BUCKET + "/purple/tumor.purple.qc", "tumor.purple.qc"),
                input(TestInputs.SOMATIC_BUCKET + "/purple/tumor.purple.purity.tsv", "tumor.purple.purity.tsv"),
                input(TestInputs.TUMOR_BUCKET + "/bam_metrics/tumor" + BAM_METRICS_SUMMARY_TSV, "tumor" + BAM_METRICS_SUMMARY_TSV),
                input(TestInputs.SOMATIC_BUCKET + "/virusbreakend/tumor.virusbreakend.vcf.summary.tsv",
                        "tumor.virusbreakend.vcf.summary.tsv"));
    }

    @Override
    protected List<AddDatatype> expectedFurtherOperations() {
        return List.of(new AddDatatype(DataType.VIRUS_INTERPRETATION,
                TestInputs.defaultSomaticRunMetadata().barcode(),
                new ArchivePath(Folder.root(), VirusInterpreter.NAMESPACE, ANNOTATED_VIRUS_TSV)));
    }

    @Override
    protected void validateOutput(final VirusInterpreterOutput output) {
        assertThat(output.maybeVirusAnnotations()).isEqualTo(Optional.of(GoogleStorageLocation.of(TestInputs.SOMATIC_BUCKET + "/virusintrprtr",
                ResultsDirectory.defaultDirectory().path(ANNOTATED_VIRUS_TSV))));
    }

    @Override
    protected void validatePersistedOutput(final VirusInterpreterOutput output) {
        assertThat(output.virusAnnotations()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET,
                "set/virusintrprtr/" + ANNOTATED_VIRUS_TSV));
    }

    @Override
    protected void setupPersistedDataset() {
        persistedDataset.addPath(DataType.VIRUS_INTERPRETATION, "virusintrprtr/" + ANNOTATED_VIRUS_TSV);
    }

    @Override
    protected void validatePersistedOutputFromPersistedDataset(final VirusInterpreterOutput output) {
        assertThat(output.virusAnnotations()).isEqualTo(GoogleStorageLocation.of(OUTPUT_BUCKET, "virusintrprtr/" + ANNOTATED_VIRUS_TSV));
    }

    @Override
    protected List<String> expectedCommands() {
        return List.of(
                toolCommand(HmfTool.VIRUS_INTERPRETER)
                        + " -sample tumor"
                        + " -purple_dir /data/input"
                        + " -tumor_metrics_dir /data/input"
                        + " -virus_breakend_tsv /data/input/tumor.virusbreakend.vcf.summary.tsv"
                        + " -taxonomy_db_tsv /opt/resources/virus_interpreter/taxonomy_db.tsv"
                        + " -virus_reporting_db_tsv /opt/resources/virus_interpreter/virus_reporting_db.tsv"
                        + " -virus_blacklisting_db_tsv /opt/resources/virus_interpreter/virus_blacklisting_db.tsv"
                        + " -output_dir /data/output");
    }
}