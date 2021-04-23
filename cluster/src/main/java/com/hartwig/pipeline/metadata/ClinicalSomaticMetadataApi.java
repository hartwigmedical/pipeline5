package com.hartwig.pipeline.metadata;

import static java.util.Optional.ofNullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hartwig.api.RunApi;
import com.hartwig.api.SampleApi;
import com.hartwig.api.model.Ini;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunFailure;
import com.hartwig.api.model.RunSet;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleType;
import com.hartwig.api.model.Status;
import com.hartwig.api.model.UpdateRun;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClinicalSomaticMetadataApi implements SomaticMetadataApi {

    private final static Logger LOGGER = LoggerFactory.getLogger(SomaticMetadataApi.class);
    public static final String FINISHED = "Finished";
    public static final String FAILED = "Failed";
    public static final String PROCESSING = "Processing";
    private static final String PIPELINE_SOURCE = "Pipeline";
    private final Run run;
    private final RunApi runApi;
    private final SampleApi sampleApi;
    private final StagedOutputPublisher stagedOutputPublisher;

    ClinicalSomaticMetadataApi(final Run run, final RunApi runApi, final SampleApi sampleApi,
            final StagedOutputPublisher stagedOutputPublisher) {
        this.runApi = runApi;
        this.sampleApi = sampleApi;
        this.stagedOutputPublisher = stagedOutputPublisher;
        this.run = run;
    }

    @Override
    public SomaticRunMetadata get() {
        String runBucket = ofNullable(run.getBucket()).orElseThrow();
        RunSet set = run.getSet();
        List<Sample> samplesBySet = sampleApi.list(null, null, null, set.getId(), null, null);

        SingleSampleRunMetadata reference = find(SampleType.REF, samplesBySet).map(referenceSample -> toMetadata(referenceSample,
                run,
                SingleSampleRunMetadata.SampleType.REFERENCE))
                .orElseThrow(() -> new IllegalStateException(String.format("No reference sample found in SBP for set [%s]",
                        set.getName())));
        String ini = run.getIni();
        if (Ini.SINGLE_INI.getValue().equals(ini)) {
            LOGGER.info("Somatic run is using single sample configuration. No algorithms will be run, just transfer and cleanup");
            return SomaticRunMetadata.builder().bucket(runBucket).set(set.getName()).reference(reference).build();
        } else {
            SingleSampleRunMetadata tumor = find(SampleType.TUMOR, samplesBySet).map(referenceSample -> toMetadata(referenceSample,
                    run,
                    SingleSampleRunMetadata.SampleType.TUMOR))
                    .orElseThrow((() -> new IllegalStateException(String.format(
                            "No tumor sample found in SBP for set [%s] and this run was not marked as single sample",
                            set.getName()))));
            return SomaticRunMetadata.builder().bucket(runBucket).set(set.getName()).reference(reference).maybeTumor(tumor).build();
        }
    }

    private static SingleSampleRunMetadata toMetadata(final Sample sample, final Run aRun, final SingleSampleRunMetadata.SampleType type) {
        return SingleSampleRunMetadata.builder()
                .bucket(ofNullable(aRun.getBucket()).orElseThrow())
                .set(aRun.getSet().getName())
                .sampleName(sample.getName())
                .barcode(sample.getBarcode())
                .type(type)
                .primaryTumorDoids(Optional.ofNullable(sample.getPrimaryTumorDoids()).orElse(Collections.emptyList()))
                .entityId(sample.getId())
                .build();
    }

    private Optional<Sample> find(final SampleType type, final List<Sample> samplesBySet) {
        return samplesBySet.stream().filter(sample -> type.equals(sample.getType())).findFirst();
    }

    @Override
    public void complete(final PipelineState pipelineState, final SomaticRunMetadata metadata) {
        if (run.getBucket() != null) {
            LOGGER.info("Recording pipeline completion with status [{}]", pipelineState.status());
            try {
                if (pipelineState.status() != PipelineStatus.FAILED) {
                    stagedOutputPublisher.publish(pipelineState, metadata);
                }
                runApi.update(run.getId(), statusUpdate(pipelineState.status()));
            } catch (Exception e) {
                runApi.update(run.getId(), statusUpdate(PipelineStatus.FAILED));
                throw e;
            }
        } else {
            throw new IllegalStateException(String.format(
                    "Run [%s] has no predefined bucket for output. The bucket should be populated externally to P5, check configuration in "
                            + "SBP API.",
                    run.getId()));
        }
    }

    public UpdateRun statusUpdate(final PipelineStatus status) {
        switch (status) {
            case FAILED:
                return new UpdateRun().status(Status.FAILED)
                        .failure(new RunFailure().type(RunFailure.TypeEnum.TECHNICALFAILURE).source(PIPELINE_SOURCE));
            case QC_FAILED:
                return new UpdateRun().status(Status.FAILED)
                        .failure(new RunFailure().type(RunFailure.TypeEnum.QCFAILURE).source(PIPELINE_SOURCE));
            default:
                return new UpdateRun().status(Status.FINISHED);
        }
    }

    @Override
    public void start() {
        UpdateRun update = new UpdateRun().failure(null).status(Status.PROCESSING);
        try {
            LOGGER.info("Updating API with run status and result [{}]", ObjectMappers.get().writeValueAsString(update));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        runApi.update(run.getId(), update);
    }
}