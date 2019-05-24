package com.hartwig.pipeline.tertiary.healthcheck;

import static java.lang.String.format;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentPair;
import com.hartwig.pipeline.alignment.after.metrics.BamMetricsOutput;
import com.hartwig.pipeline.calling.somatic.SomaticCallerOutput;
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

import org.jetbrains.annotations.NotNull;

public class HealthChecker {

    static final String NAMESPACE = "health_checker";
    private static final String METRICS_FILE_FORMAT = "%s/QCStats/%s/%s_WGSMetrics.txt";
    private static final String PURPLE_DIR = "purple";
    private static final String AMBER_DIR = "amber";
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
            SomaticCallerOutput somaticCallerOutput, PurpleOutput purpleOutput, AmberOutput amberOutput, String setName) {

        if (!arguments.runTertiary()) {
            return HealthCheckOutput.builder().status(JobStatus.SKIPPED).build();
        }

        String tumorSampleName = pair.tumor().sample().name();
        String referenceSampleName = pair.reference().sample().name();
        RuntimeBucket runtimeBucket = RuntimeBucket.from(storage, NAMESPACE, referenceSampleName, tumorSampleName, arguments);

        BashStartupScript bash = BashStartupScript.of(runtimeBucket.name());

        InputDownload metricsDownload = downloadMetrics(metricsOutput);
        InputDownload mateMetricsDownload = downloadMetrics(mateMetricsOutput);
        InputDownload somaticVcfDownload = new InputDownload(somaticCallerOutput.finalSomaticVcf(),
                format("%s/%s_post_processed.vcf.gz", VmDirectories.INPUT, tumorSampleName));
        InputDownload purpleDownload = new InputDownload(purpleOutput.outputDirectory(), format("%s/%s", VmDirectories.INPUT, PURPLE_DIR));
        InputDownload amberDownload = new InputDownload(amberOutput.outputDirectory(), format("%s/%s", VmDirectories.INPUT, AMBER_DIR));

        String healthCheckOutput = "/HealthCheck.out";
        String outputPath = VmDirectories.OUTPUT + healthCheckOutput;
        bash.addCommand(new MkDirCommand(inputDir(PURPLE_DIR)))
                .addCommand(new MkDirCommand(inputDir(AMBER_DIR)))
                .addCommand(metricsDownload)
                .addCommand(mateMetricsDownload)
                .addCommand(somaticVcfDownload)
                .addCommand(purpleDownload)
                .addCommand(amberDownload)
                .addCommand(new GenerateMetadataCommand(VmDirectories.INPUT, setName, pair))
                .addCommand(new HealthCheckerApplicationCommand(VmDirectories.INPUT, outputPath))
                .addCommand(new HealthCheckEvaluationCommand(VmDirectories.OUTPUT, healthCheckOutput))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path())));
        JobStatus status = computeEngine.submit(runtimeBucket, VirtualMachineJobDefinition.healthChecker(bash, resultsDirectory));
        return HealthCheckOutput.builder()
                .status(status)
                .maybeOutputFile((GoogleStorageLocation.of(runtimeBucket.name(), resultsDirectory.path(healthCheckOutput))))
                .addReportComponents(new EntireOutputComponent(runtimeBucket, pair, NAMESPACE, resultsDirectory))
                .build();
    }

    @NotNull
    private String inputDir(final String dir) {
        return VmDirectories.INPUT + "/" + dir;
    }

    @NotNull
    private InputDownload downloadMetrics(final BamMetricsOutput metricsOutput) {
        return new InputDownload(metricsOutput.metricsOutputFile(),
                format(METRICS_FILE_FORMAT, VmDirectories.INPUT, metricsOutput.sample().name(), metricsOutput.sample().name()));
    }
}
