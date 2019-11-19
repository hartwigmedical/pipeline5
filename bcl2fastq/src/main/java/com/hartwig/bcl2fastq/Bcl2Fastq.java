package com.hartwig.bcl2fastq;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.storage.StorageProvider;

class Bcl2Fastq {

    private final Storage storage;
    private final ComputeEngine computeEngine;
    private final Bcl2fastqArguments arguments;
    private final ResultsDirectory resultsDirectory;

    Bcl2Fastq(final Storage storage, final ComputeEngine computeEngine, final Bcl2fastqArguments arguments,
            final ResultsDirectory resultsDirectory) {
        this.storage = storage;
        this.computeEngine = computeEngine;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
    }

    void run() {
        FlowcellMetadata metadata = FlowcellMetadata.from(arguments);
        RuntimeBucket bucket = RuntimeBucket.from(storage, "bcl2fastq", metadata, arguments);
        BashStartupScript bash = BashStartupScript.of(bucket.name());
        BclDownload bclDownload = new BclDownload(arguments.inputBucket(), arguments.flowcell());
        bash.addCommand(bclDownload)
                .addCommand(new Bcl2FastqCommand(bclDownload.getLocalTargetPath(), VmDirectories.OUTPUT))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));
        computeEngine.submit(bucket, VirtualMachineJobDefinition.bcl2fastq(bash, resultsDirectory));

        Map<String, List<Blob>> samples = bucket.list(resultsDirectory.path("Test"))
                .stream()
                .collect(Collectors.groupingBy(blob -> new File(blob.getName()).getName().split("_")[0]));

        for (Map.Entry<String, List<Blob>> sample : samples.entrySet()) {
            for (Blob blob : sample.getValue()) {
                String sampleId = sample.getKey();
                storage.copy(Storage.CopyRequest.of(blob.getBucket(),
                        blob.getName(),
                        BlobInfo.newBuilder(arguments.outputBucket(), sampleId + "/" + new File(blob.getName()).getName()).build()));
            }
        }
    }

    public static void main(String[] args) {
        try {
            Bcl2fastqArguments arguments = Bcl2fastqArguments.from(args);
            GoogleCredentials credentials = CredentialProvider.from(arguments).get();
            new Bcl2Fastq(StorageProvider.from(arguments, credentials).get(),
                    ComputeEngine.from(arguments, credentials, false),
                    arguments,
                    ResultsDirectory.defaultDirectory()).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
