package com.hartwig.bcl2fastq;

import static java.lang.String.format;

import java.io.File;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.hartwig.bcl2fastq.metadata.ImmutableSbpSample;
import com.hartwig.bcl2fastq.metadata.SbpFastq;
import com.hartwig.bcl2fastq.metadata.SbpFastqMetadataApi;
import com.hartwig.bcl2fastq.metadata.SbpFlowcell;
import com.hartwig.bcl2fastq.metadata.SbpLane;
import com.hartwig.bcl2fastq.metadata.SbpSample;
import com.hartwig.bcl2fastq.qc.QualityControl;
import com.hartwig.bcl2fastq.qc.QualityControlResults;
import com.hartwig.bcl2fastq.samplesheet.SampleSheet;
import com.hartwig.bcl2fastq.samplesheet.SampleSheetCsv;
import com.hartwig.bcl2fastq.stats.Aggregations;
import com.hartwig.bcl2fastq.stats.LaneStats;
import com.hartwig.bcl2fastq.stats.ReadMetrics;
import com.hartwig.bcl2fastq.stats.SampleStats;
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

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Bcl2Fastq {

    private static final Logger LOGGER = LoggerFactory.getLogger(Bcl2Fastq.class);
    private final Storage storage;
    private final ComputeEngine computeEngine;
    private final Bcl2fastqArguments arguments;
    private final ResultsDirectory resultsDirectory;
    private final QualityControl qc;
    private final SbpFastqMetadataApi sbpFastqMetadataApi;

    private Bcl2Fastq(final Storage storage, final ComputeEngine computeEngine, final Bcl2fastqArguments arguments,
            final ResultsDirectory resultsDirectory, final QualityControl qc, final SbpFastqMetadataApi sbpFastqMetadataApi) {
        this.storage = storage;
        this.computeEngine = computeEngine;
        this.arguments = arguments;
        this.resultsDirectory = resultsDirectory;
        this.qc = qc;
        this.sbpFastqMetadataApi = sbpFastqMetadataApi;
    }

    private void run() {
        LOGGER.info("Starting bcl2fastq for flowcell [{}]", arguments.flowcell());
        FlowcellMetadata metadata = FlowcellMetadata.from(arguments);
        RuntimeBucket bucket = RuntimeBucket.from(storage, "bcl2fastq", metadata, arguments);
        BashStartupScript bash = BashStartupScript.of(bucket.name());
        BclDownload bclDownload = new BclDownload(arguments.inputBucket(), arguments.flowcell());
        bash.addCommand(bclDownload)
                .addCommand(new Bcl2FastqCommand(bclDownload.getLocalTargetPath(), VmDirectories.OUTPUT))
                .addCommand(new OutputUpload(GoogleStorageLocation.of(bucket.name(), resultsDirectory.path())));
        computeEngine.submit(bucket, VirtualMachineJobDefinition.bcl2fastq(bash, resultsDirectory));

        SampleSheet sampleSheet = new SampleSheetCsv(storage.get(arguments.inputBucket()), arguments.flowcell()).read();
        Stats stats = new StatsJson(stringOf(bucket, "/Stats/Stats.json")).stats();
        QualityControlResults qcResults = qc.evaluate(stats, stringOf(bucket, "/run.log"));

        SbpFlowcell sbpFlowcell = sbpFastqMetadataApi.getFlowcell(sampleSheet.experimentName());
        Map<String, SbpLane> lanes = new HashMap<>();
        for (LaneStats lane : stats.conversionResults()) {
            long yield = lane.demuxResults().stream().mapToLong(SampleStats::yield).sum();
            long yieldQ30 = lane.demuxResults().stream().flatMap(s -> s.readMetrics().stream()).mapToLong(ReadMetrics::yieldQ30).sum();
            SbpLane sbpLane = SbpLane.builder()
                    .flowcell_id(sbpFlowcell.id())
                    .name(lane(lane.laneNumber()))
                    .yld(yield)
                    .q30((double) yieldQ30 / yield)
                    .build();
            lanes.put(sbpLane.name(), sbpFastqMetadataApi.findOrCreate(sbpLane));
        }

        boolean undet_rds_p_pass = qcResults.flowcellPasses(arguments.flowcell());
        if (undet_rds_p_pass) {
            for (String project : sampleSheet.projects()) {
                LOGGER.info("Copying converted FASTQ into output bucket [{}] for project [{}]", arguments.outputBucket(), project);
                Conversion conversionResult = Conversion.from(bucket.list(resultsDirectory.path(project))
                        .stream()
                        .map(Blob::getName)
                        .collect(Collectors.toList()));

                for (ConvertedSample sample : conversionResult.samples()) {
                    SbpSample sbpSample = sbpFastqMetadataApi.findOrCreate(sample.barcode(), project);
                    for (ConvertedFastq fastq : sample.fastq()) {
                        boolean fastqPassesQC = qcResults.fastqPasses(fastq.id());

                        Blob r1Blob = copy(bucket, sample, arguments.flowcell(), fastq.pathR1());
                        Blob r2Blob = copy(bucket, sample, arguments.flowcell(), fastq.pathR2());

                        SbpLane sbpLane = lanes.get(lane(fastq.id().lane()));
                        int lane_id = sbpLane.id().orElseThrow();

                        SbpFastq sbpFastq = SbpFastq.builder()
                                .sample_id(sbpSample.id().orElseThrow())
                                .lane_id(lane_id)
                                .bucket(arguments.outputBucket())
                                .name_r1(r1Blob.getName())
                                .size_r1(r1Blob.getSize())
                                .hash_r1(convertMd5ToSbpFormat(r1Blob.getMd5()))
                                .name_r2(r2Blob.getName())
                                .size_r2(r2Blob.getSize())
                                .hash_r2(convertMd5ToSbpFormat(r2Blob.getMd5()))
                                .qc_pass(fastqPassesQC)
                                .build();

                        sbpFastqMetadataApi.findOrCreate(sbpFastq);
                    }
                    final long sampleYield = Aggregations.yield(sbpSample.barcode(), stats);
                    final double sampleQ30 = Aggregations.yieldQ30(sbpSample.barcode(), stats) / (double) sampleYield * 100;
                    ImmutableSbpSample.Builder sampleUpdate = SbpSample.builder().from(sbpSample).yld(sampleYield).q30(sampleQ30);
                    if (sbpSample.yld_req().map(yr -> yr < sampleYield).orElse(true) && sbpSample.q30_req()
                            .map(qr -> qr < sampleQ30)
                            .orElse(true)) {
                        sampleUpdate.status("Ready");
                    } else {
                        sampleUpdate.status("Insufficient Quality");
                    }
                    sbpFastqMetadataApi.updateSample(sampleUpdate.build());
                }
            }
        }

        SbpFlowcell updated = sbpFastqMetadataApi.updateFlowcell(SbpFlowcell.builderFrom(sbpFlowcell)
                .status("Converted")
                .undet_rds_p_pass(undet_rds_p_pass)
                .build());
        SbpFlowcell withTimestamp =
                sbpFastqMetadataApi.updateFlowcell(SbpFlowcell.builderFrom(updated).convertTime(updated.updateTime()).build());
        LOGGER.info("Updated flowcell [{}]", withTimestamp);
    }

    private String convertMd5ToSbpFormat(String originalMd5) {
        return new String(Hex.encodeHex(Base64.getDecoder().decode(originalMd5)));
    }

    private String lane(final int laneNumber) {
        return format("L00%s", laneNumber);
    }

    private String stringOf(final RuntimeBucket bucket, final String blobName) {
        return new String(bucket.get(resultsDirectory.path(blobName)).getContent());
    }

    private Blob copy(final RuntimeBucket bucket, final ConvertedSample sample, final String flowcell, final String path) {
        return storage.copy(Storage.CopyRequest.of(bucket.bucket().getName(),
                path,
                BlobInfo.newBuilder(arguments.outputBucket(), flowcell + "/" + sample.barcode() + "/" + new File(path).getName()).build()))
                .getResult();
    }

    public static void main(String[] args) {
        try {
            Bcl2fastqArguments arguments = Bcl2fastqArguments.from(args);
            GoogleCredentials credentials = CredentialProvider.from(arguments).get();
            new Bcl2Fastq(StorageProvider.from(arguments, credentials).get(),
                    ComputeEngine.from(arguments, credentials),
                    arguments,
                    ResultsDirectory.defaultDirectory(),
                    QualityControl.defaultQC(),
                    SbpFastqMetadataApi.newInstance(arguments.sbpApiUrl())).run();
        } catch (Exception e) {
            LOGGER.error("Unable to run bcl2fastq", e);
        }
    }
}
