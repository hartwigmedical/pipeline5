package com.hartwig.pipeline.tertiary.healthcheck;

import static java.lang.String.format;

import static com.hartwig.pipeline.tools.HmfTool.HEALTH_CHECKER;

import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hartwig.computeengine.execution.vm.BashStartupScript;
import com.hartwig.computeengine.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.execution.vm.command.InputDownloadCommand;
import com.hartwig.computeengine.execution.vm.command.unix.MkDirCommand;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.computeengine.storage.ResultsDirectory;
import com.hartwig.computeengine.storage.RuntimeBucket;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineStatus;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinitions;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.output.AddDatatype;
import com.hartwig.pipeline.output.EntireOutputComponent;
import com.hartwig.pipeline.output.Folder;
import com.hartwig.pipeline.output.RunLogComponent;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Namespace(HealthChecker.NAMESPACE)
public class HealthChecker implements Stage<HealthCheckOutput, SomaticRunMetadata> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthChecker.class);

    public static final String NAMESPACE = "health_checker";

    private static final String LOCAL_METRICS_DIR = VmDirectories.INPUT + "/metrics";
    private static final String LOCAL_PURPLE_DIR = VmDirectories.INPUT + "/purple";

    private final InputDownloadCommand referenceMetricsDownload;
    private final InputDownloadCommand tumorMetricsDownload;
    private final InputDownloadCommand purpleDownload;

    public HealthChecker(
            final BamMetricsOutput referenceMetricsOutput, final BamMetricsOutput tumorMetricsOutput, final PurpleOutput purpleOutput) {

        referenceMetricsDownload = new InputDownloadCommand(
                referenceMetricsOutput.outputLocations().outputDirectory(), LOCAL_METRICS_DIR);

        tumorMetricsDownload = new InputDownloadCommand(
                tumorMetricsOutput.outputLocations().outputDirectory(), LOCAL_METRICS_DIR);

        purpleDownload = new InputDownloadCommand(purpleOutput.outputLocations().outputDirectory(), LOCAL_PURPLE_DIR);
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(
                new MkDirCommand(LOCAL_METRICS_DIR),
                new MkDirCommand(LOCAL_PURPLE_DIR),
                referenceMetricsDownload,
                tumorMetricsDownload,
                purpleDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = commonArguments(LOCAL_PURPLE_DIR, VmDirectories.OUTPUT);
        arguments.addAll(tumorArguments(metadata.tumor().sampleName(), tumorMetricsDownload.getLocalTargetPath()));
        return List.of(formCommand(arguments));
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = commonArguments(LOCAL_PURPLE_DIR, VmDirectories.OUTPUT);
        arguments.addAll(referenceArguments(metadata.reference().sampleName(), referenceMetricsDownload.getLocalTargetPath()));
        return List.of(formCommand(arguments));
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = commonArguments(LOCAL_PURPLE_DIR, VmDirectories.OUTPUT);
        arguments.addAll(tumorArguments(metadata.tumor().sampleName(), tumorMetricsDownload.getLocalTargetPath()));
        arguments.addAll(referenceArguments(metadata.reference().sampleName(), referenceMetricsDownload.getLocalTargetPath()));
        return List.of(formCommand(arguments));
    }

    private List<String> commonArguments(final String purplePath, final String outputPath) {

        List<String> arguments = Lists.newArrayList();
        arguments.add(format("-purple_dir %s", purplePath));
        arguments.add(format("-output_dir %s", outputPath));
        return arguments;
    }

    private List<String> tumorArguments(final String tumorSampleName, final String tumorMetricsPath) {

        List<String> arguments = Lists.newArrayList();
        arguments.add(format("-tumor %s", tumorSampleName));
        arguments.add(format("-tumor_metrics_dir %s", tumorMetricsPath));
        return arguments;
    }

    private List<String> referenceArguments(final String referenceSampleName, final String referenceMetricsPath) {

        List<String> arguments = Lists.newArrayList();
        arguments.add(format("-reference %s", referenceSampleName));
        arguments.add(format("-ref_metrics_dir %s", referenceMetricsPath));
        return arguments;
    }

    private BashCommand formCommand(final List<String> arguments)
    {
        return JavaCommandFactory.javaJarCommand(HEALTH_CHECKER, arguments);
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinitions.healthChecker(bash, resultsDirectory);
    }

    @Override
    public HealthCheckOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return HealthCheckOutput.builder()
                .status(checkHealthCheckerOutput(metadata.sampleName(), bucket, jobStatus, resultsDirectory))
                .addFailedLogLocations(GoogleStorageLocation.of(bucket.name(), RunLogComponent.LOG_FILE))
                .maybeOutputDirectory(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path()))
                .addReportComponents(new EntireOutputComponent(bucket, Folder.root(), NAMESPACE, resultsDirectory))
                .build();
    }

    @Override
    public HealthCheckOutput skippedOutput(final SomaticRunMetadata metadata) {
        return HealthCheckOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public HealthCheckOutput persistedOutput(final SomaticRunMetadata metadata) {
        return HealthCheckOutput.builder().status(PipelineStatus.PERSISTED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary()
                && !arguments.shallow()
                && arguments.biopsy().isEmpty()
                && !arguments.context().equals(Pipeline.Context.RESEARCH)
                && !arguments.context().equals(Pipeline.Context.RESEARCH2)
                && !arguments.useTargetRegions();
    }

    @NotNull
    private PipelineStatus checkHealthCheckerOutput(final String tumorSampleName, final RuntimeBucket runtimeBucket, PipelineStatus status,
            final ResultsDirectory resultsDirectory) {

        List<Blob> healthCheckStatuses = runtimeBucket.list(resultsDirectory.path(tumorSampleName));

        if ((status == PipelineStatus.SKIPPED || status == PipelineStatus.SUCCESS) && healthCheckStatuses.size() == 1) {
            Blob healthCheckStatus = healthCheckStatuses.get(0);
            if (healthCheckStatus.getName().endsWith("HealthCheckSucceeded")) {
                LOGGER.debug("Health check reported success");
                status = PipelineStatus.SUCCESS;
            } else if (healthCheckStatus.getName().endsWith("HealthCheckFailed")) {
                LOGGER.warn("Health check reported failure. Check run.log in health checker out for reason");
                status = PipelineStatus.QC_FAILED;
            } else {
                LOGGER.warn(
                        "Health check completed with unknown status [{}]. Failing the run. Check run.log in health checker out for more "
                                + "detail",
                        healthCheckStatus.getName());
                status = PipelineStatus.FAILED;

            }
        } else {
            LOGGER.error("Found [{}] files in the health checker output. Unable to determine status, this is likely a bug in the pipeline",
                    healthCheckStatuses.size());
            status = PipelineStatus.FAILED;
        }
        return status;
    }

    @Override
    public List<AddDatatype> addDatatypes(final SomaticRunMetadata metadata) {
        throw new UnsupportedOperationException("No datatypes for HealthChecker");
    }
}
