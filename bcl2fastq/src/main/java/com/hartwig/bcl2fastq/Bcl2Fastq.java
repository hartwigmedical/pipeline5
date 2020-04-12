package com.hartwig.bcl2fastq;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.bcl2fastq.conversion.ResultAggregation;
import com.hartwig.bcl2fastq.forensics.ForensicArchive;
import com.hartwig.bcl2fastq.metadata.FastqMetadataRegistration;
import com.hartwig.bcl2fastq.metadata.SbpFastqMetadataApi;
import com.hartwig.bcl2fastq.metadata.SbpFlowcell;
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
import com.hartwig.pipeline.tools.Versions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bcl2Fastq {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bcl2Fastq.class);
    public static final String RUN_LOG = "run.log";
    private final Storage storage;
    private final ComputeEngine computeEngine;
    private final Bcl2fastqArguments arguments;
    private final ResultsDirectory resultsDirectory;
    private final SbpFastqMetadataApi sbpFastqMetadataApi;
    private final ForensicArchive forensicArchive;

    private Bcl2Fastq(final Storage storage, final ComputeEngine computeEngine, final Bcl2fastqArguments arguments,
            final ResultsDirectory resultsDirectory, final SbpFastqMetadataApi sbpFastqMetadataApi, final ForensicArchive forensicArchive) {
        this.storage = storage;
        this.computeEngine = computeEngine;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
        this.sbpFastqMetadataApi = sbpFastqMetadataApi;
        this.forensicArchive = forensicArchive;
    }

    private void run() throws Exception{
        Versions.printAll();
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

        LOGGER.info("Storing forensics for later analysis");
        forensicArchive.store(flowcellPath, bucket, inputBucket, RUN_LOG);
        new OutputCopier(arguments,
                bucket,
                new GsUtilFacade(arguments.cloudSdkPath(),
                        arguments.outputProject(),
                        arguments.outputPrivateKeyPath())).andThen(new FastqMetadataRegistration(sbpFastqMetadataApi,
                arguments.outputBucket(),
                stringOf(bucket, "/" + RUN_LOG))).accept(new ResultAggregation(bucket, resultsDirectory).apply(sampleSheet, stats));


        if (arguments.cleanup()) {
            LOGGER.info("Cleaning up conversion inputs and runtime buckets.");
            if (arguments.privateKeyPath().isPresent()) {
                GSUtil.auth(arguments.cloudSdkPath(), arguments.privateKeyPath().get());
            }
            GSUtil.rm(arguments.cloudSdkPath(), bucket.runId());
        }
        LOGGER.info("bcl2fastq complete for flowcell [{}]", arguments.flowcell());
    }

    private ImmutableVirtualMachineJobDefinition jobDefinition(final BashStartupScript bash) {
        return ImmutableVirtualMachineJobDefinition.builder()
                .name("bcl2fastq")
                .startupCommand(bash)
                .performanceProfile(VirtualMachinePerformanceProfile.custom(96, 512))
                .namespacedResults(resultsDirectory)
                .workingDiskSpaceGb(10000)
                .build();
    }

    private String stringOf(final RuntimeBucket bucket, final String blobName) {
        return new String(resultBlobOf(bucket, blobName).getContent());
    }

    private Blob resultBlobOf(final RuntimeBucket bucket, final String blobName) {
        return bucket.get(resultsDirectory.path(blobName));
    }

    public static void main(String[] args) {
        Bcl2fastqArguments arguments = Bcl2fastqArguments.from(args);
        SbpFastqMetadataApi api = SbpFastqMetadataApi.newInstance(arguments.sbpApiUrl());
        try {
            GoogleCredentials credentials = CredentialProvider.from(arguments).get();
            Storage storage = StorageProvider.from(arguments, credentials).get();
            new Bcl2Fastq(storage,
                    ComputeEngine.from(arguments, credentials),
                    arguments,
                    ResultsDirectory.defaultDirectory(),
                    api,
                    new ForensicArchive(arguments.forensicBucket(), arguments)).run();
            System.exit(0);
        } catch (Exception e) {
            api.updateFlowcell(SbpFlowcell.builderFrom(api.getFlowcell(arguments.flowcell())).status("Failed").build());
            LOGGER.error("Unable to run bcl2fastq", e);
            System.exit(1);
        }
    }
}
