package com.hartwig.pipeline.metadata;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.sbpapi.SbpIni;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.sbpapi.SbpRun;
import com.hartwig.pipeline.sbpapi.SbpRunFailure;
import com.hartwig.pipeline.sbpapi.SbpRunResultUpdate;
import com.hartwig.pipeline.sbpapi.SbpSample;
import com.hartwig.pipeline.sbpapi.SbpSet;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClinicalSomaticMetadataApi implements SomaticMetadataApi {

    private static final String REF = "ref";
    private static final String TUMOR = "tumor";
    private final static Logger LOGGER = LoggerFactory.getLogger(SomaticMetadataApi.class);
    private static final String SINGLE_SAMPLE_INI = "SingleSample";
    public static final String FINISHED = "Finished";
    public static final String FAILED = "Failed";
    public static final String PROCESSING = "Processing";
    private final Arguments arguments;
    private final int sbpRunId;
    private final SbpRestApi sbpRestApi;
    private final StagedOutputPublisher stagedOutputPublisher;

    ClinicalSomaticMetadataApi(final Arguments arguments, final int sbpRunId, final SbpRestApi sbpRestApi,
            final StagedOutputPublisher stagedOutputPublisher) {
        this.arguments = arguments;
        this.sbpRunId = sbpRunId;
        this.sbpRestApi = sbpRestApi;
        this.stagedOutputPublisher = stagedOutputPublisher;
    }

    @Override
    public SomaticRunMetadata get() {
        try {
            SbpRun sbpRun = getSbpRun();
            SbpSet sbpSet = sbpRun.set();
            List<SbpSample> samplesBySet = ObjectMappers.get().readValue(sbpRestApi.getSample(sbpSet.id()), new TypeReference<>() {
            });

            SingleSampleRunMetadata reference = find(REF, samplesBySet).map(referenceSample -> toMetadata(referenceSample,
                    sbpRun,
                    SingleSampleRunMetadata.SampleType.REFERENCE))
                    .orElseThrow(() -> new IllegalStateException(String.format("No reference sample found in SBP for set [%s]",
                            sbpSet.name())));
            SbpIni ini = findIni(sbpRun, getInis());
            if (ini.name().startsWith(SINGLE_SAMPLE_INI)) {
                LOGGER.info("Somatic run is using single sample configuration. No algorithms will be run, just transfer and cleanup");
                return SomaticRunMetadata.builder().bucket(sbpRun.bucket().orElseThrow()).set(sbpSet.name()).reference(reference).build();
            } else {
                SingleSampleRunMetadata tumor = find(TUMOR, samplesBySet).map(referenceSample -> toMetadata(referenceSample,
                        sbpRun,
                        SingleSampleRunMetadata.SampleType.TUMOR))
                        .orElseThrow((() -> new IllegalStateException(String.format(
                                "No tumor sample found in SBP for set [%s] and this run was not marked as single sample",
                                sbpSet.name()))));
                return SomaticRunMetadata.builder()
                        .bucket(sbpRun.bucket().orElseThrow())
                        .set(sbpSet.name())
                        .reference(reference)
                        .maybeTumor(tumor)
                        .build();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<SbpIni> getInis() throws IOException {
        return ObjectMappers.get().readValue(sbpRestApi.getInis(), new TypeReference<>() {
        });
    }

    private SbpIni findIni(final SbpRun sbpRun, final List<SbpIni> inis) {
        return inis.stream()
                .filter(i -> i.id() == sbpRun.ini_id())
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(String.format(
                        "Unable to find ini [%s] referenced in run [%s]. " + "Check the configuration in the SBP API",
                        sbpRun.ini_id(),
                        sbpRun.id())));
    }

    private static SingleSampleRunMetadata toMetadata(final SbpSample sample, final SbpRun aRun,
            final SingleSampleRunMetadata.SampleType type) {
        return SingleSampleRunMetadata.builder()
                .bucket(aRun.bucket().orElseThrow())
                .set(aRun.set().name())
                .sampleName(sample.name())
                .barcode(sample.barcode())
                .entityId(sample.id())
                .type(type)
                .primaryTumorDoids(sample.primary_tumor_doids())
                .build();
    }

    private SbpRun getSbpRun() {
        try {
            return ObjectMappers.get().readValue(sbpRestApi.getRun(sbpRunId), SbpRun.class);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private Optional<SbpSample> find(final String type, final List<SbpSample> samplesBySet) {
        return samplesBySet.stream().filter(sample -> sample.type().equals(type)).findFirst();
    }

    @Override
    public void complete(final PipelineState pipelineState, final SomaticRunMetadata metadata) {
        String runIdAsString = String.valueOf(sbpRunId);
        SbpRun sbpRun = getSbpRun();
        String sbpBucket = sbpRun.bucket().orElse(null);

        if (sbpBucket != null) {
            LOGGER.info("Recording pipeline completion with status [{}]", pipelineState.status());
            try {
                if (pipelineState.status() != PipelineStatus.FAILED) {
                    stagedOutputPublisher.publish(pipelineState, metadata);
                }
                sbpRestApi.updateRunResult(runIdAsString, statusUpdate(pipelineState.status()));
            } catch (Exception e) {
                sbpRestApi.updateRunResult(runIdAsString, statusUpdate(PipelineStatus.FAILED));
                throw e;
            }
        } else {
            throw new IllegalStateException(String.format(
                    "Run [%s] has no predefined bucket for output. The bucket should be populated externally to P5, check configuration in "
                            + "SBP API.",
                    sbpRunId));
        }
    }

    public SbpRunResultUpdate statusUpdate(final PipelineStatus status) {
        switch (status) {
            case FAILED:
            case QC_FAILED:
                return SbpRunResultUpdate.builder()
                        .status(FAILED)
                        .bucket(arguments.archiveBucket())
                        .failure(SbpRunFailure.from(status))
                        .build();
            default:
                return SbpRunResultUpdate.builder().status(FINISHED).bucket(arguments.archiveBucket()).build();
        }
    }

    @Override
    public void start() {
        SbpRunResultUpdate update =
                SbpRunResultUpdate.builder().bucket(arguments.archiveBucket()).failure(Optional.empty()).status(PROCESSING).build();
        try {
            LOGGER.info("Updating API with run status and result [{}]", ObjectMappers.get().writeValueAsString(update));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        sbpRestApi.updateRunResult(String.valueOf(sbpRunId), update);
    }
}