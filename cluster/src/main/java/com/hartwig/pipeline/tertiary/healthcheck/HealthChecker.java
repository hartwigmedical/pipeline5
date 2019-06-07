package com.hartwig.pipeline.tertiary.healthcheck;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.MkDirCommand;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.tertiary.amber.AmberOutput;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;
import com.hartwig.pipeline.trace.StageTrace;

public class HealthChecker {

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

    public HealthCheckOutput run(AlignmentPair pair, BamMetricsOutput metricsOutput, BamMetricsOutput mateMetricsOutput,
            AmberOutput amberOutput, PurpleOutput purpleOutput) {
        if (!arguments.runTertiary()) {
            return HealthCheckOutput.builder().status(JobStatus.SKIPPED).build();
        }

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.COMPUTE_ENGINE).start();

        String referenceSampleName = pair.reference().sample().name();
        String tumorSampleName = pair.tumor().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, referenceSampleName, tumorSampleName, arguments);

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
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.healthChecker(bash, resultsDirectory));
        trace.stop();
        return HealthCheckOutput.builder()
                .status(status)
                .maybeOutputDirectory(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path()))
                .addReportComponents(new EntireOutputComponent(runtimeBucket, pair, NAMESPACE, resultsDirectory))
                .build();
    }

    private static String localMetricsPath(BamMetricsOutput metricsOutput) {
        return LOCAL_METRICS_DIR + "/" + metricsOutput.sample().name() + ".wgsmetrics";
    }
}
