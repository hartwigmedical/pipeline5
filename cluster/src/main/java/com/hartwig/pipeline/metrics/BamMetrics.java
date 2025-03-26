package com.hartwig.pipeline.metrics;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.ArchivePath;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.output.StartupScriptComponent;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.reruns.PersistedLocations;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;

import org.jetbrains.annotations.NotNull;

@Namespace(BamMetrics.NAMESPACE)
public class BamMetrics implements Stage<BamMetricsOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "bam_metrics";
    public static final String BAM_METRICS_SUMMARY_TSV = ".bam_metric.summary.tsv";
    public static final String BAM_METRICS_COVERAGE_TSV = ".bam_metric.coverage.tsv";
    public static final String BAM_METRICS_FRAG_LENGTH_TSV = ".bam_metric.frag_length.tsv";
    public static final String BAM_METRICS_FLAG_COUNT_TSV = ".bam_metric.flag_counts.tsv";
    public static final String BAM_METRICS_PARTITION_STATS_TSV = ".bam_metric.partition_stats.tsv";

    private final ResourceFiles resourceFiles;
    private final InputDownloadCommand bamDownload;
    private final InputDownloadCommand bamBaiDownload;
    private final PersistedDataset persistedDataset;
    private final Arguments arguments;

    public BamMetrics(final ResourceFiles resourceFiles, final AlignmentOutput alignmentOutput, final PersistedDataset persistedDataset,
            final Arguments arguments) {
        this.resourceFiles = resourceFiles;
        bamDownload = new InputDownloadCommand(alignmentOutput.alignments());
        bamBaiDownload = new InputDownloadCommand(alignmentOutput.alignments().transform(FileTypes::toAlignmentIndex));
        this.persistedDataset = persistedDataset;
        this.arguments = arguments;
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runBamMetrics();
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(bamDownload, bamBaiDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SingleSampleRunMetadata metadata) {
        return bamMetricsCommands(metadata);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SingleSampleRunMetadata metadata) {
        return bamMetricsCommands(metadata);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SingleSampleRunMetadata metadata) {
        return bamMetricsCommands(metadata);
    }

    public List<BashCommand> bamMetricsCommands(final SingleSampleRunMetadata metadata) {
        ArrayList<BashCommand> bashCommands = new ArrayList<>();

        bashCommands.add(new BamMetricsCommand(metadata.sampleName(),
                bamDownload.getLocalTargetPath(),
                resourceFiles,
                VmDirectories.OUTPUT,
                Bash.allCpus(),
                arguments.useTargetRegions() ? resourceFiles.targetRegionsBed() : null));

        return bashCommands;
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript script, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.bamMetrics(script, resultsDirectory);
    }

    @Override
    public BamMetricsOutput output(final SingleSampleRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        return BamMetricsOutput.builder()
                .status(jobStatus)
                .sample(metadata.sampleName())
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeOutputLocations(BamMetricsOutputLocations.builder()
                        .outputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(), true))
                        .summary(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(summaryTsv(metadata))))
                        .coverage(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(coverageTsv(metadata))))
                        .fragmentLengths(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(fragLengthTsv(metadata))))
                        .flagCounts(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(flagCountTsv(metadata))))
                        .partitionStats(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path(partitionStatsTsv(metadata))))
                        .build())
                .addReportComponents(new RunLogComponent(bucket, namespace(), Folder.from(metadata), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, namespace(), Folder.from(metadata)))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.from(metadata), namespace(), resultsDirectory))
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    private String summaryTsv(final SingleSampleRunMetadata metadata) {
        return metadata.sampleName() + BAM_METRICS_SUMMARY_TSV;
    }

    private String coverageTsv(final SingleSampleRunMetadata metadata) {
        return metadata.sampleName() + BAM_METRICS_COVERAGE_TSV;
    }

    private String fragLengthTsv(final SingleSampleRunMetadata metadata) {
        return metadata.sampleName() + BAM_METRICS_FRAG_LENGTH_TSV;
    }

    private String flagCountTsv(final SingleSampleRunMetadata metadata) {
        return metadata.sampleName() + BAM_METRICS_FLAG_COUNT_TSV;
    }

    private String partitionStatsTsv(final SingleSampleRunMetadata metadata) {
        return metadata.sampleName() + BAM_METRICS_PARTITION_STATS_TSV;
    }

    @Override
    public BamMetricsOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return BamMetricsOutput.builder().sample(metadata.sampleName()).status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public BamMetricsOutput persistedOutput(final SingleSampleRunMetadata metadata) {
        String summmaryTsv = summaryTsv(metadata);
        return BamMetricsOutput.builder()
                .status(PipelineStatus.PERSISTED)
                .sample(metadata.sampleName())
                .maybeOutputLocations(BamMetricsOutputLocations.builder()
                        .summary(persistedOrDefault(metadata, DataType.METRICS_SUMMARY, summmaryTsv))
                        .coverage(persistedOrDefault(metadata, DataType.METRICS_COVERAGE, coverageTsv(metadata)))
                        .fragmentLengths(persistedOrDefault(metadata, DataType.METRICS_FRAG_LENGTH, fragLengthTsv(metadata)))
                        .flagCounts(persistedOrDefault(metadata, DataType.METRICS_FLAG_COUNT, flagCountTsv(metadata)))
                        .partitionStats(persistedOrDefault(metadata, DataType.METRICS_PARTITION, partitionStatsTsv(metadata)))
                        .outputDirectory(persistedOrDefault(metadata,
                                DataType.METRICS_SUMMARY,
                                summmaryTsv).transform(f -> new File(f).getParent()).asDirectory())
                        .build())
                .addAllDatatypes(addDatatypes(metadata))
                .build();
    }

    @NotNull
    public GoogleStorageLocation persistedOrDefault(final SingleSampleRunMetadata metadata, final DataType dataType, final String path) {
        return persistedDataset.path(metadata.sampleName(), dataType)
                .orElse(GoogleStorageLocation.of(metadata.bucket(),
                        PersistedLocations.blobForSingle(metadata.set(), metadata.sampleName(), namespace(), path)));
    }

    @Override
    public List<AddDatatype> addDatatypes(final SingleSampleRunMetadata metadata) {
        return List.of(new AddDatatype(DataType.METRICS_SUMMARY,
                        metadata.barcode(),
                        new ArchivePath(Folder.from(metadata), namespace(), summaryTsv(metadata))),
                new AddDatatype(DataType.METRICS_COVERAGE,
                        metadata.barcode(),
                        new ArchivePath(Folder.from(metadata), namespace(), coverageTsv(metadata))),
                new AddDatatype(DataType.METRICS_FRAG_LENGTH,
                        metadata.barcode(),
                        new ArchivePath(Folder.from(metadata), namespace(), fragLengthTsv(metadata))),
                new AddDatatype(DataType.METRICS_FLAG_COUNT,
                        metadata.barcode(),
                        new ArchivePath(Folder.from(metadata), namespace(), flagCountTsv(metadata))),
                new AddDatatype(DataType.METRICS_PARTITION,
                        metadata.barcode(),
                        new ArchivePath(Folder.from(metadata), namespace(), partitionStatsTsv(metadata))));
    }
}
