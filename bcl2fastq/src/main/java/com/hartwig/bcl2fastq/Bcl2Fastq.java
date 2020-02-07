package com.hartwig.bcl2fastq;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.bcl2fastq.conversion.ResultAggregation;
import com.hartwig.bcl2fastq.metadata.FastqMetadataRegistration;
import com.hartwig.bcl2fastq.metadata.SbpFastqMetadataApi;
import com.hartwig.bcl2fastq.samplesheet.SampleSheet;
import com.hartwig.bcl2fastq.samplesheet.SampleSheetCsv;
import com.hartwig.bcl2fastq.stats.Stats;
import com.hartwig.bcl2fastq.stats.StatsJson;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.credentials.CredentialProvider;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.ComputeEngine;
import com.hartwig.pipeline.execution.vm.ImmutableVirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VirtualMachinePerformanceProfile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.storage.GSUtil;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.GsUtilFacade;
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

        FlowcellMetadata metadata = FlowcellMetadata.from(arguments);
        RuntimeBucket bucket = RuntimeBucket.from(storage, "bcl2fastq", metadata, arguments);
        BashStartupScript bash = BashStartupScript.of(bucket.name());
        Bucket inputBucket = storage.get(arguments.inputBucket());
        String flowcellPath = InputPath.resolve(inputBucket, arguments.flowcell());
        LOGGER.info("Resolved input BCL path to [{}]", flowcellPath);
        BclDownload bclDownload = new BclDownload(inputBucket, flowcellPath);
        VirtualMachineJobDefinition jobDefinition = jobDefinition(bash);
        bash.addCommand(bclDownload)
                .addCommand(new Bcl2FastqCommand(bclDownload.getLocalTargetPath(),
                        VmDirectories.OUTPUT,
                        jobDefinition.performanceProfile().machineType().cpus()))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));
        computeEngine.submit(bucket, jobDefinition);

        LOGGER.info("Conversion complete. Starting post-processing.");
        SampleSheet sampleSheet = new SampleSheetCsv(inputBucket, flowcellPath).read();
        Stats stats = new StatsJson(stringOf(bucket, "/Stats/Stats.json")).stats();

        new OutputCopier(arguments,
                bucket,
                new GsUtilFacade(arguments.cloudSdkPath(),
                        arguments.outputProject(),
                        arguments.outputPrivateKeyPath())).andThen(new FastqMetadataRegistration(sbpFastqMetadataApi,
                arguments.outputBucket(),
                stringOf(bucket, "/run.log"))).accept(new ResultAggregation(bucket, resultsDirectory).apply(sampleSheet, stats));



        if (arguments.cleanup()) {
            LOGGER.info("Cleaning up conversion inputs and runtime buckets.");
            GSUtil.rm(arguments.cloudSdkPath(), bucket.runId());
            GSUtil.rm(arguments.cloudSdkPath(), inputBucket.getName() + "/" + flowcellPath);
        }
        LOGGER.info("bcl2fastq complete for flowcell [{}]", arguments.flowcell());
    }

    private ImmutableVirtualMachineJobDefinition jobDefinition(final BashStartupScript bash) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("bcl2fastq")
                .startupCommand(bash)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(96, 90))
                .namespacedResults(resultsDirectory)
                .workingDiskSpaceGb(10000)
                .build();
    }

    private String stringOf(final RuntimeBucket bucket, final String blobName) {
        return new String(bucket.get(resultsDirectory.path(blobName)).getContent());
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
