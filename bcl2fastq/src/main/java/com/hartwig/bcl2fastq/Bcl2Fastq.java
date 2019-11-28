package com.hartwig.bcl2fastq;

import java.io.File;
import java.util.stream.Collectors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.hartwig.bcl2fastq.metadata.ConversionMetadataApi;
import com.hartwig.bcl2fastq.qc.QualityControl;
import com.hartwig.bcl2fastq.qc.QualityControlResults;
import com.hartwig.bcl2fastq.samplesheet.SampleSheet;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bcl2Fastq {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bcl2Fastq.class);
    private final Storage storage;
    private final ComputeEngine computeEngine;
    private final Bcl2fastqArguments arguments;
    private final ResultsDirectory resultsDirectory;
    private final QualityControl qc;
    private final ConversionMetadataApi conversionMetadataApi;

    private Bcl2Fastq(final Storage storage, final ComputeEngine computeEngine, final Bcl2fastqArguments arguments,
            final ResultsDirectory resultsDirectory, final QualityControl qc, final ConversionMetadataApi conversionMetadataApi) {
        this.storage = storage;
        this.computeEngine = computeEngine;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
        this.qc = qc;
        this.conversionMetadataApi = conversionMetadataApi;
    }

    private void run() {
        LOGGER.info("Starting bcl2fastq for flowcell [{}]", arguments.flowcell());
        SampleSheet sampleSheet = new SampleSheet(storage.get(arguments.inputBucket()), arguments.flowcell());
        FlowcellMetadata metadata = FlowcellMetadata.from(arguments);
        RuntimeBucket bucket = RuntimeBucket.from(storage, "bcl2fastq", metadata, arguments);
        BashStartupScript bash = BashStartupScript.of(bucket.name());
        BclDownload bclDownload = new BclDownload(arguments.inputBucket(), arguments.flowcell());
        bash.addCommand(bclDownload)
                .addCommand(new Bcl2FastqCommand(bclDownload.getLocalTargetPath(), VmDirectories.OUTPUT))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));
        computeEngine.submit(bucket, VirtualMachineJobDefinition.bcl2fastq(bash, resultsDirectory));

        QualityControlResults qcResults = qc.evaluate(stringOf(bucket, "/Stats/Stats.json"), stringOf(bucket, "/run.log"));

        if (qcResults.flowcellPasses()) {
            for (String project : sampleSheet.projects()) {
                LOGGER.info("Copying converted FASTQ into output bucket [{}] for project [{}]", arguments.outputBucket(), project);
                Conversion conversionResult = Conversion.from(bucket.list(resultsDirectory.path(project))
                        .stream()
                        .map(Blob::getName)
                        .collect(Collectors.toList()));

                for (ConvertedSample sample : conversionResult.samples()) {
                    for (ConvertedFastq fastq : sample.fastq()) {
                        copy(bucket, sample, fastq.pathR1());
                        copy(bucket, sample, fastq.pathR2());
                    }
                }
            }
        } else {
            LOGGER.warn("Flowcell [{}] did not pass QC", arguments.flowcell());
        }
    }

    private String stringOf(final RuntimeBucket bucket, final String blobName) {
        return new String(bucket.get(resultsDirectory.path(blobName)).getContent());
    }

    private void copy(final RuntimeBucket bucket, final ConvertedSample sample, final String path) {
        storage.copy(Storage.CopyRequest.of(bucket.bucket().getName(),
                path,
                BlobInfo.newBuilder(arguments.outputBucket(), sample.barcode() + "/" + new File(path).getName()).build())).getResult();
    }

    public static void main(String[] args) {
        try {
            Bcl2fastqArguments arguments = Bcl2fastqArguments.from(args);
            GoogleCredentials credentials = CredentialProvider.from(arguments).get();
            new Bcl2Fastq(StorageProvider.from(arguments, credentials).get(),
                    ComputeEngine.from(arguments, credentials, false),
                    arguments,
                    ResultsDirectory.defaultDirectory(),
                    QualityControl.defaultQC(),
                    null).run();
        } catch (Exception e) {
            LOGGER.error("Unable to run bcl2fastq", e);
        }
    }
}
