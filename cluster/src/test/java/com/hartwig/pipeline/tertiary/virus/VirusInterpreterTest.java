package com.hartwig.pipeline.tertiary.virus;

import static com.hartwig.pipeline.testsupport.TestInputs.SOMATIC_BUCKET;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

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

import org.junit.Before;

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
        return List.of(input(expectedRuntimeBucketName() + "/purple/tumor.purple.qc", "tumor.purple.qc"),
                input(expectedRuntimeBucketName() + "/purple/tumor.purple.purity.tsv", "tumor.purple.purity.tsv"),
                input("run-tumor-test/bam_metrics/results/tumor.wgsmetrics", "tumor.wgsmetrics"),
                input(expectedRuntimeBucketName() + "/virusbreakend/tumor.virusbreakend.vcf.summary.tsv",
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
        assertThat(output.maybeVirusAnnotations()).isEqualTo(Optional.of(GoogleStorageLocation.of(SOMATIC_BUCKET + "/virusintrprtr",
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
        return List.of("java -Xmx2G -jar /opt/tools/virus-interpreter/1.2/virus-interpreter.jar " + "-sample_id tumor "
                + "-purple_purity_tsv /data/input/tumor.purple.purity.tsv " + "-purple_qc_file /data/input/tumor.purple.qc "
                + "-tumor_sample_wgs_metrics_file /data/input/tumor.wgsmetrics "
                + "-virus_breakend_tsv /data/input/tumor.virusbreakend.vcf.summary.tsv "
                + "-taxonomy_db_tsv /opt/resources/virus_interpreter/taxonomy_db.tsv "
                + "-virus_reporting_db_tsv /opt/resources/virus_interpreter/virus_reporting_db.tsv " + "-output_dir /data/output");
    }
}