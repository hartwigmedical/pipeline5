package com.hartwig.pipeline.tertiary.healthcheck;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;

public class HealthChecker {

    private static final String NAMESPACE = "health_checker";
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

    public HealthCheckOutput run(AlignmentPair pair, GoogleStorageLocation metricsOutput, GoogleStorageLocation mateMetricsOutput,
            GoogleStorageLocation somaticVcf, GoogleStorageLocation purpleOutput, GoogleStorageLocation amberOutput) {
        RuntimeBucket runtimeBucket =
                RuntimeBucket.from(storage, NAMESPACE, pair.reference().sample().name(), pair.tumor().sample().name(), arguments);

        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        InputDownload metricsDownload = new InputDownload(metricsOutput);
        InputDownload mateMetricsDownload = new InputDownload(mateMetricsOutput);
        InputDownload somaticVcfDownload = new InputDownload(somaticVcf);
        InputDownload purpleDownload = new InputDownload(purpleOutput);
        InputDownload amberDownload = new InputDownload(amberOutput);

        bash.addCommand(metricsDownload)
                .addCommand(mateMetricsDownload)
                .addCommand(somaticVcfDownload)
                .addCommand(purpleDownload)
                .addCommand(amberDownload)
                .addCommand(new HealthCheckerApplicationCommand(VmDirectories.INPUT, VmDirectories.OUTPUT))
                .addCommand(new HealthCheckEvaluationCommand(VmDirectories.OUTPUT))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.healthChecker(bash, resultsDirectory));
        return HealthCheckOutput.builder()
                .status(status)
                .outputFile((GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path("HealthCheck.out"))))
                .build();
    }
}
