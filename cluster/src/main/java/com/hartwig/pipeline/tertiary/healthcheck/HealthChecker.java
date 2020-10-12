package com.hartwig.pipeline.tertiary.healthcheck;

import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.metrics.BamMetricsOutput;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthChecker implements Stage<HealthCheckOutput, SomaticRunMetadata> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthChecker.class);
    public static final String NAMESPACE = "health_checker";
    private static final String LOCAL_METRICS_DIR = VmDirectories.INPUT + "/metrics";
    private static final String LOCAL_AMBER_DIR = VmDirectories.INPUT + "/amber";
    private static final String LOCAL_PURPLE_DIR = VmDirectories.INPUT + "/purple";
    private final InputDownload referenceMetricsDownload;
    private final InputDownload tumorMetricsDownload;
    private final InputDownload amberDownload;
    private final InputDownload purpleDownload;

    public HealthChecker(BamMetricsOutput referenceMetricsOutput, BamMetricsOutput tumorMetricsOutput, AmberOutput amberOutput,
            PurpleOutput purpleOutput) {
        referenceMetricsDownload = new InputDownload(referenceMetricsOutput.metricsOutputFile(), localMetricsPath(referenceMetricsOutput));
        tumorMetricsDownload = new InputDownload(tumorMetricsOutput.metricsOutputFile(), localMetricsPath(tumorMetricsOutput));
        amberDownload = new InputDownload(amberOutput.outputDirectory(), LOCAL_AMBER_DIR);
        purpleDownload = new InputDownload(purpleOutput.outputDirectory(), LOCAL_PURPLE_DIR);
    }

    @Override
    public List<BashCommand> inputs() {
        return ImmutableList.of(new MkDirCommand(LOCAL_METRICS_DIR),
                new MkDirCommand(LOCAL_AMBER_DIR),
                new MkDirCommand(LOCAL_PURPLE_DIR),
                referenceMetricsDownload,
                tumorMetricsDownload,
                amberDownload,
                purpleDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return ImmutableList.of(new HealthCheckerApplicationCommand(metadata.reference().sampleName(),
                metadata.tumor().sampleName(),
                LOCAL_METRICS_DIR,
                LOCAL_AMBER_DIR,
                LOCAL_PURPLE_DIR,
                VmDirectories.OUTPUT));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.healthChecker(bash, resultsDirectory);
    }

    @Override
    public HealthCheckOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {

        return HealthCheckOutput.builder()
                .status(checkHealthCheckerOutput(metadata.tumor().sampleName(), bucket, jobStatus, resultsDirectory))
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
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }

    @NotNull
    private PipelineStatus checkHealthCheckerOutput(final String tumorSampleName, final RuntimeBucket runtimeBucket, PipelineStatus status,
            ResultsDirectory resultsDirectory) {
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

    private static String localMetricsPath(BamMetricsOutput metricsOutput) {
        return LOCAL_METRICS_DIR + "/" + metricsOutput.sample() + ".wgsmetrics";
    }
}
