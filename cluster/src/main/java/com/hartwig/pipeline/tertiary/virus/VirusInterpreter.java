package com.hartwig.pipeline.tertiary.virus;

import static com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile.custom;
import static com.hartwig.pipeline.tools.ToolInfo.VIRUS_INTERPRETER;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.SingleFileComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.TertiaryStage;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.tools.Versions;

@Namespace(VirusInterpreter.NAMESPACE)
public class VirusInterpreter extends TertiaryStage<VirusInterpreterOutput> {

    public static final String NAMESPACE = "virusintrprtr";

    private final ResourceFiles resourceFiles;
    private final PersistedDataset persistedDataset;
    private final InputDownload virusBreakendOutput;
    private final InputDownload tumorBamMetrics;
    private final InputDownload purpleQc;
    private final InputDownload purplePurity;

    public VirusInterpreter(final AlignmentPair alignmentPair, final ResourceFiles resourceFiles, final PersistedDataset persistedDataset,
            final VirusBreakendOutput virusBreakendOutput, final PurpleOutput purpleOutput, final BamMetricsOutput tumorBamMetricsOutput) {
        super(alignmentPair);
        this.resourceFiles = resourceFiles;
        this.persistedDataset = persistedDataset;
        this.virusBreakendOutput = new InputDownload(virusBreakendOutput.summary());
        this.purpleQc = new InputDownload(purpleOutput.outputLocations().qcFile());
        this.purplePurity = new InputDownload(purpleOutput.outputLocations().purity());
        this.tumorBamMetrics = InputDownload.initialiseOptionalLocation(tumorBamMetricsOutput.maybeMetricsOutputFile());
    }

    @Override
    public List<BashCommand> inputs() {
        return List.of(virusBreakendOutput, purplePurity, purpleQc, tumorBamMetrics);
    }

    @Override
    public String namespace() {
        return VirusInterpreter.NAMESPACE;
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.useTargetRegions();
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return generateCommands(metadata);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        return generateCommands(metadata);
    }

    private List<BashCommand> generateCommands(final SomaticRunMetadata metadata) {
        return List.of(new JavaJarCommand(
                VIRUS_INTERPRETER,
                List.of("-sample_id",
                        metadata.tumor().sampleName(),
                        "-purple_purity_tsv",
                        purplePurity.getLocalTargetPath(),
                        "-purple_qc_file",
                        purpleQc.getLocalTargetPath(),
                        "-tumor_sample_wgs_metrics_file",
                        tumorBamMetrics.getLocalTargetPath(),
                        "-virus_breakend_tsv",
                        virusBreakendOutput.getLocalTargetPath(),
                        "-taxonomy_db_tsv",
                        resourceFiles.virusInterpreterTaxonomyDb(),
                        "-virus_reporting_db_tsv",
                        resourceFiles.virusReportingDb(),
                        "-output_dir",
                        VmDirectories.OUTPUT)));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript startupScript, final ResultsDirectory resultsDirectory) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name(NAMESPACE)
                .startupCommand(startupScript)
                .namespacedResults(resultsDirectory)
                .performanceProfile(custom(2, 8))
                .build();
    }

    @Override
    public VirusInterpreterOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        String annotatedTsv = annotatedVirusTsv(metadata);
        return VirusInterpreterOutput.builder().status(jobStatus).addAllDatatypes(addDatatypes(metadata))
                .maybeVirusAnnotations(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(annotatedTsv)))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .addReportComponents(new SingleFileComponent(bucket,
                        NAMESPACE,
                        Folder.root(),
                        annotatedTsv,
                        annotatedTsv,
                        resultsDirectory), new RunLogComponent(bucket, NAMESPACE, Folder.root(), resultsDirectory))
                .build();
    }

    @Override
    public VirusInterpreterOutput skippedOutput(final SomaticRunMetadata metadata) {
        return VirusInterpreterOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public VirusInterpreterOutput persistedOutput(final SomaticRunMetadata metadata) {
        return VirusInterpreterOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .maybeVirusAnnotations(persistedDataset.path(metadata.tumor().sampleName(), DataType.VIRUS_INTERPRETATION)
                        .orElse(GoogleStorageLocation.of(metadata.bucket(),
                                PersistedLocations.blobForSet(metadata.set(), namespace(), annotatedVirusTsv(metadata)))))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.VIRUS_INTERPRETATION,
                metadata.barcode(),
                new ArchivePath(Folder.root(), namespace(), annotatedVirusTsv(metadata))));
    }

    private String annotatedVirusTsv(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + ".virus.annotated.tsv";
    }
}
