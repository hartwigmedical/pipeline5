package com.hartwig.pipeline.alignment.bwa;

import static java.lang.String.format;

import static com.hartwig.pipeline.datatypes.FileTypes.bai;
import static com.hartwig.pipeline.datatypes.FileTypes.bam;
import static com.hartwig.pipeline.resource.ResourceFilesFactory.buildResourceFiles;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import com.google.cloud.storage.Storage;
import com.hartwig.patient.Lane;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.Aligner;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.alignment.ImmutableAlignmentOutput;
import com.hartwig.pipeline.alignment.sample.SampleSource;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.failsafe.DefaultBackoffPolicy;
import com.hartwig.pipeline.metadata.AddDatatype;
import com.hartwig.pipeline.metadata.ArchivePath;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.ReportComponent;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStageInputOutput;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.SampleUpload;
import com.hartwig.pipeline.trace.StageTrace;

import net.jodah.failsafe.Failsafe;

public class BwaAligner implements Aligner {

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final SampleSource sampleSource;
    private final SampleUpload sampleUpload;
    private final ResultsDirectory resultsDirectory;
    private final ExecutorService executorService;

    public BwaAligner(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage, final SampleSource sampleSource,
            final SampleUpload sampleUpload, final ResultsDirectory resultsDirectory, final ExecutorService executorService) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.sampleSource = sampleSource;
        this.sampleUpload = sampleUpload;
        this.resultsDirectory = resultsDirectory;
        this.executorService = executorService;
    }

    public AlignmentOutput run(final SingleSampleRunMetadata metadata) throws Exception {

        StageTrace trace = new StageTrace(NAMESPACE, metadata.sampleName(), StageTrace.ExecutorType.COMPUTE_ENGINE).start();
        RuntimeBucket rootBucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);

        Sample sample = sampleSource.sample(metadata);
        if (sample.bam().isPresent()) {
            String noPrefix = sample.bam().orElseThrow().replace("gs://", "");
            int firstSlash = noPrefix.indexOf("/");
            String bucket = noPrefix.substring(0, firstSlash);
            String path = noPrefix.substring(firstSlash + 1);
            return AlignmentOutput.builder()
                    .sample(metadata.sampleName())
                    .status(PipelineStatus.PROVIDED)
                    .maybeFinalBamLocation(GoogleStorageLocation.of(bucket, path))
                    .maybeFinalBaiLocation(GoogleStorageLocation.of(bucket, FileTypes.bai(path)))
                    .build();
        }
        final ResourceFiles resourceFiles = buildResourceFiles(arguments);
        sampleUpload.run(sample, rootBucket);

        List<Future<PipelineStatus>> futures = new ArrayList<>();
        List<GoogleStorageLocation> perLaneBams = new ArrayList<>();
        List<ReportComponent> laneLogComponents = new ArrayList<>();
        List<GoogleStorageLocation> laneFailedLogs = new ArrayList<>();
        for (Lane lane : sample.lanes()) {

            RuntimeBucket laneBucket = RuntimeBucket.from(storage, laneNamespace(lane), metadata, arguments);

            BashStartupScript bash = BashStartupScript.of(laneBucket.name());

            InputDownload first =
                    new InputDownload(GoogleStorageLocation.of(rootBucket.name(), fastQFileName(sample.name(), lane.firstOfPairPath())));
            InputDownload second =
                    new InputDownload(GoogleStorageLocation.of(rootBucket.name(), fastQFileName(sample.name(), lane.secondOfPairPath())));
            bash.addCommand(first).addCommand(second);

            SubStageInputOutput alignment = new LaneAlignment(arguments.sbpApiRunId().isPresent(),
                    resourceFiles.refGenomeFile(),
                    first.getLocalTargetPath(),
                    second.getLocalTargetPath(),
                    metadata.sampleName(),
                    lane).apply(SubStageInputOutput.empty(metadata.sampleName()));
            perLaneBams.add(GoogleStorageLocation.of(laneBucket.name(), resultsDirectory.path(alignment.outputFile().fileName())));

            bash.addCommands(alignment.bash())
                    .addCommand(new OutputUpload(GoogleStorageLocation.of(laneBucket.name(), resultsDirectory.path()),
                            RuntimeFiles.typical()));
            futures.add(executorService.submit(() -> runWithRetries(metadata,
                    laneBucket,
                    VirtualMachineJobDefinition.alignment(laneId(lane).toLowerCase(), bash, resultsDirectory))));
            laneLogComponents.add(new RunLogComponent(laneBucket, laneNamespace(lane), Folder.from(metadata), resultsDirectory));
            laneFailedLogs.add(GoogleStorageLocation.of(laneBucket.name(), RunLogComponent.LOG_FILE));
        }

        AlignmentOutput output;
        if (lanesSuccessfullyComplete(futures)) {

            List<InputDownload> laneBams = perLaneBams.stream().map(InputDownload::new).collect(Collectors.toList());

            BashStartupScript mergeMarkdupsBash = BashStartupScript.of(rootBucket.name());
            laneBams.forEach(mergeMarkdupsBash::addCommand);

            SubStageInputOutput merged = new MergeMarkDups(laneBams.stream()
                    .map(InputDownload::getLocalTargetPath)
                    .filter(path -> path.endsWith("bam"))
                    .collect(Collectors.toList())).apply(SubStageInputOutput.empty(metadata.sampleName()));

            mergeMarkdupsBash.addCommands(merged.bash());

            mergeMarkdupsBash.addCommand(new OutputUpload(GoogleStorageLocation.of(rootBucket.name(), resultsDirectory.path()),
                    RuntimeFiles.typical()));

            PipelineStatus status =
                    runWithRetries(metadata, rootBucket, VirtualMachineJobDefinition.mergeMarkdups(mergeMarkdupsBash, resultsDirectory));

            ImmutableAlignmentOutput.Builder outputBuilder = AlignmentOutput.builder()
                    .sample(metadata.sampleName())
                    .status(status)
                    .maybeFinalBamLocation(GoogleStorageLocation.of(rootBucket.name(),
                            resultsDirectory.path(merged.outputFile().fileName())))
                    .maybeFinalBaiLocation(GoogleStorageLocation.of(rootBucket.name(),
                            resultsDirectory.path(bai(merged.outputFile().fileName()))))
                    .addAllReportComponents(laneLogComponents)
                    .addAllFailedLogLocations(laneFailedLogs)
                    .addFailedLogLocations(GoogleStorageLocation.of(rootBucket.name(), RunLogComponent.LOG_FILE))
                    .addReportComponents(new RunLogComponent(rootBucket, Aligner.NAMESPACE, Folder.from(metadata), resultsDirectory));
            if (!arguments.outputCram()) {
                outputBuilder.addReportComponents(new SingleFileComponent(rootBucket,
                                Aligner.NAMESPACE,
                                Folder.from(metadata),
                                bam(metadata.sampleName()),
                                bam(metadata.sampleName()),
                                resultsDirectory),
                        new SingleFileComponent(rootBucket,
                                Aligner.NAMESPACE,
                                Folder.from(metadata),
                                bai(bam(metadata.sampleName())),
                                bai(bam(metadata.sampleName())),
                                resultsDirectory))
                        .addDatatypes(new AddDatatype(DataType.ALIGNED_READS,
                                        metadata.barcode(),
                                        new ArchivePath(Folder.from(metadata), BwaAligner.NAMESPACE, bam(metadata.sampleName()))),
                                new AddDatatype(DataType.ALIGNED_READS_INDEX,
                                        metadata.barcode(),
                                        new ArchivePath(Folder.from(metadata), BwaAligner.NAMESPACE, bai(metadata.sampleName()))));
            }
            output = outputBuilder.build();
        } else {
            output = AlignmentOutput.builder().sample(metadata.sampleName()).status(PipelineStatus.FAILED).build();
        }
        trace.stop();
        executorService.shutdown();
        return output;
    }

    public PipelineStatus runWithRetries(final SingleSampleRunMetadata metadata, final RuntimeBucket laneBucket,
            final VirtualMachineJobDefinition jobDefinition) {
        return Failsafe.with(DefaultBackoffPolicy.of(String.format("[%s] stage [%s]", metadata.toString(), Aligner.NAMESPACE)))
                .get(() -> computeEngine.submit(laneBucket, jobDefinition));
    }

    private static String laneNamespace(final Lane lane) {
        return NAMESPACE + "/" + laneId(lane);
    }

    static String laneId(final Lane lane) {
        return lane.flowCellId() + "-" + lane.laneNumber();
    }

    private boolean lanesSuccessfullyComplete(final List<Future<PipelineStatus>> futures) {
        return futures.stream().map(BwaAligner::getFuture).noneMatch(status -> status.equals(PipelineStatus.FAILED));
    }

    private static String fastQFileName(final String sample, final String fullFastQPath) {
        return format("samples/%s/%s", sample, new File(fullFastQPath).getName());
    }

    private static PipelineStatus getFuture(Future<PipelineStatus> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
