package com.hartwig.bcl2fastq;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

class Bcl2Fastq {

    private final Storage storage;
    private final ComputeEngine computeEngine;
    private final Arguments arguments;
    private final ResultsDirectory resultsDirectory;

    Bcl2Fastq(final Storage storage, final ComputeEngine computeEngine, final Arguments arguments,
            final ResultsDirectory resultsDirectory) {
        this.storage = storage;
        this.computeEngine = computeEngine;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
    }

    void run() {
        FlowcellMetadata metadata = FlowcellMetadata.from(arguments);
        RuntimeBucket bucket =
                RuntimeBucket.from(storage, "bcl2fastq", metadata, com.hartwig.pipeline.Arguments.defaults("development"));
        BashStartupScript bash = BashStartupScript.of(bucket.name());
        BclDownload bclDownload = new BclDownload(arguments.inputBucket(), arguments.flowcell());
        bash.addCommand(bclDownload)
                .addCommand(new Bcl2FastqCommand(bclDownload.getLocalTargetPath(), VmDirectories.OUTPUT))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));
        computeEngine.submit(bucket, VirtualMachineJobDefinition.bcl2fastq(bash, resultsDirectory));
    }
}
