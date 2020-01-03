package com.hartwig.bcl2fastq;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.bcl2fastq.metadata.FastqMetadataRegistration;
import com.hartwig.bcl2fastq.metadata.SbpFastqMetadataApi;
import com.hartwig.bcl2fastq.results.ResultAggregation;
import com.hartwig.bcl2fastq.samplesheet.SampleSheet;
import com.hartwig.bcl2fastq.samplesheet.SampleSheetCsv;
import com.hartwig.bcl2fastq.stats.Stats;
import com.hartwig.bcl2fastq.stats.StatsJson;
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
    private final SbpFastqMetadataApi sbpFastqMetadataApi;

    private Bcl2Fastq(final Storage storage, final ComputeEngine computeEngine, final Bcl2fastqArguments arguments,
            final ResultsDirectory resultsDirectory, final SbpFastqMetadataApi sbpFastqMetadataApi) {
        this.storage = storage;
        this.computeEngine = computeEngine;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
        this.sbpFastqMetadataApi = sbpFastqMetadataApi;
    }

    private void run() {
        LOGGER.info("Starting bcl2fastq for flowcell [{}]", arguments.flowcell());

        // run bcl2fastq
        FlowcellMetadata metadata = FlowcellMetadata.from(arguments);
        RuntimeBucket bucket = RuntimeBucket.from(storage, "bcl2fastq", metadata, arguments);
        BashStartupScript bash = BashStartupScript.of(bucket.name());
        BclDownload bclDownload = new BclDownload(arguments.inputBucket(), arguments.flowcell());
        bash.addCommand(bclDownload)
                .addCommand(new Bcl2FastqCommand(bclDownload.getLocalTargetPath(), VmDirectories.OUTPUT))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));
        computeEngine.submit(bucket, VirtualMachineJobDefinition.bcl2fastq(bash, resultsDirectory));

        SampleSheet sampleSheet = new SampleSheetCsv(storage.get(arguments.inputBucket()), arguments.flowcell()).read();
        Stats stats = new StatsJson(new String(bucket.get(resultsDirectory.path("/Stats/Stats.json")).getContent())).stats();

        new OutputCopy(storage, arguments.outputBucket(), bucket).andThen(new FastqMetadataRegistration(sbpFastqMetadataApi,
                arguments.outputBucket())).accept(new ResultAggregation(bucket.getUnderlyingBucket()).apply(sampleSheet, stats));

        LOGGER.info("bcl2fastq complete for flowcell [{}]", arguments.flowcell());
    }

    public static void main(String[] args) {
        try {
            Bcl2fastqArguments arguments = Bcl2fastqArguments.from(args);
            GoogleCredentials credentials = CredentialProvider.from(arguments).get();
            new Bcl2Fastq(StorageProvider.from(arguments, credentials).get(),
                    ComputeEngine.from(arguments, credentials),
                    arguments,
                    ResultsDirectory.defaultDirectory(),
                    SbpFastqMetadataApi.newInstance(arguments.sbpApiUrl())).run();
        } catch (Exception e) {
            LOGGER.error("Unable to run bcl2fastq", e);
        }
    }
}
