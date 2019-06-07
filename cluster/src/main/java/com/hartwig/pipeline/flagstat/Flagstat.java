package com.hartwig.pipeline.flagstat;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.SubShellCommand;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.io.GoogleStorageLocation;
import com.hartwig.pipeline.io.ResultsDirectory;
import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.trace.StageTrace;

public class Flagstat {
    public static final String NAMESPACE = "flagstat";
    private final Arguments arguments;
    private final ComputeEngine executor;
    private final Storage storage;
    private final ResultsDirectory resultsDirectory;

    Flagstat(final Arguments arguments, final ComputeEngine executor, final Storage storage, final ResultsDirectory results) {
        this.arguments = arguments;
        this.executor = executor;
        this.storage = storage;
        this.resultsDirectory = results;
    }

    public FlagstatOutput run(AlignmentOutput alignmentOutput) {

        StageTrace trace = new StageTrace(NAMESPACE, StageTrace.ExecutorType.COMPUTE_ENGINE).start();
        RuntimeBucket bucket = RuntimeBucket.from(storage, NAMESPACE, alignmentOutput.sample().name(), arguments);

        InputDownload bamDownload = new InputDownload(alignmentOutput.finalBamLocation());

        String outputFile = FlagstatOutput.outputFile(alignmentOutput.sample());
        BashStartupScript bash = BashStartupScript.of(bucket.name())
                .addCommand(bamDownload)
                .addCommand(new SubShellCommand(new SamtoolsFlagstatCommand(bamDownload.getLocalTargetPath(),
                        VmDirectories.OUTPUT + "/" + outputFile)))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));

        JobStatus status = executor.submit(bucket, VirtualMachineJobDefinition.flagstat(bash, resultsDirectory));
        trace.stop();
        return FlagstatOutput.builder()
                .status(status)
                .addReportComponents(new RunLogComponent(bucket, Flagstat.NAMESPACE, alignmentOutput.sample().name(), resultsDirectory))
                .addReportComponents(new SingleFileComponent(bucket,
                        Flagstat.NAMESPACE,
                        alignmentOutput.sample().name(),
                        outputFile,
                        resultsDirectory))
                .build();
    }
}
