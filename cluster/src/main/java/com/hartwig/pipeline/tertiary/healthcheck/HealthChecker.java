package com.hartwig.pipeline.tertiary.healthcheck;

import java.util.List;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.MkDirCommand;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.trace.StageTrace;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthChecker {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthChecker.class);
    static final String NAMESPACE = "health_checker";
    private static final String LOCAL_METRICS_DIR = VmDirectories.INPUT + "/metrics";
    private static final String LOCAL_AMBER_DIR = VmDirectories.INPUT + "/amber";
    private static final String LOCAL_PURPLE_DIR = VmDirectories.INPUT + "/purple";

    private final Arguments arguments;
    private final ComputeEngine computeEngine;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    HealthChecker(final Arguments arguments, final ComputeEngine computeEngine, final Storage storage,
            final ResultsDirectory resultsDirectory) {
        this.arguments = arguments;
        this.computeEngine = computeEngine;
        this.storage = storage;
        this.resultsDirectory = resultsDirectory;
    }

    public HealthCheckOutput run(SomaticRunMetadata metadata, AlignmentPair pair, BamMetricsOutput metricsOutput,
            BamMetricsOutput mateMetricsOutput, AmberOutput amberOutput, PurpleOutput purpleOutput) {
        if (!arguments.runTertiary()) {
            return HealthCheckOutput.builder().status(PipelineStatus.SKIPPED).build();
        }

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        String referenceSampleName = pair.reference().sample();
        String tumorSampleName = pair.tumor().sample();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, metadata, arguments);

        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        InputDownload metricsDownload = new InputDownload(metricsOutput.metricsOutputFile(), localMetricsPath(metricsOutput));
        InputDownload mateMetricsDownload = new InputDownload(mateMetricsOutput.metricsOutputFile(), localMetricsPath(mateMetricsOutput));
        InputDownload amberDownload = new InputDownload(amberOutput.outputDirectory(), LOCAL_AMBER_DIR);
        InputDownload purpleDownload = new InputDownload(purpleOutput.outputDirectory(), LOCAL_PURPLE_DIR);

        bash.addCommand(new MkDirCommand(LOCAL_METRICS_DIR))
                .addCommand(new MkDirCommand(LOCAL_AMBER_DIR))
                .addCommand(new MkDirCommand(LOCAL_PURPLE_DIR))
                .addCommand(metricsDownload)
                .addCommand(mateMetricsDownload)
                .addCommand(amberDownload)
                .addCommand(purpleDownload)
                .addCommand(new HealthCheckerApplicationCommand(referenceSampleName,
                        tumorSampleName,
                        LOCAL_METRICS_DIR,
                        LOCAL_AMBER_DIR,
                        LOCAL_PURPLE_DIR,
                        VmDirectories.OUTPUT))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        PipelineStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.healthChecker(bash, resultsDirectory));

        status = checkHealthCheckerOutput(tumorSampleName, runtimeBucket, status);

        trace.stop();
        return HealthCheckOutput.builder()
                .status(status)
                .maybeOutputDirectory(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path()))
                .addReportComponents(new EntireOutputComponent(runtimeBucket, Folder.from(), NAMESPACE, resultsDirectory))
                .build();
    }

    @NotNull
    private PipelineStatus checkHealthCheckerOutput(final String tumorSampleName, final RuntimeBucket runtimeBucket,
            PipelineStatus status) {
        List<Blob> healthCheckStatuses = runtimeBucket.list(resultsDirectory.path(tumorSampleName));
        if ((status == PipelineStatus.SKIPPED || status == PipelineStatus.SUCCESS) && healthCheckStatuses.size() == 1) {
            Blob healthCheckStatus = healthCheckStatuses.get(0);
            if (healthCheckStatus.getName().endsWith("HealthCheckSucceeded")) {
                LOGGER.debug("Health check reported success");
                status = PipelineStatus.SUCCESS;
            } else if (healthCheckStatus.getName().endsWith("HealthCheckFailed")) {
                LOGGER.warn("Health check reported failure. Check run.log in health checker out for reason");
                status = PipelineStatus.FAILED;
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
