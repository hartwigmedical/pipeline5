package com.hartwig.pipeline.metadata;

import static java.util.Optional.ofNullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import com.hartwig.api.RunApi;
import com.hartwig.api.SampleApi;
import com.hartwig.api.model.Ini;
import com.hartwig.api.model.Run;
import com.hartwig.api.model.RunSet;
import com.hartwig.api.model.Sample;
import com.hartwig.api.model.SampleType;
import com.hartwig.pipeline.PipelineState;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.transfer.staged.StagedOutputPublisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiagnosticSomaticMetadataApi implements SomaticMetadataApi {

    private final static Logger LOGGER = LoggerFactory.getLogger(SomaticMetadataApi.class);
    public static final String FAILED = "Failed";
    private final Run run;
    private final Anonymizer anonymizer;
    private final RunApi runApi;
    private final SampleApi sampleApi;
    private final StagedOutputPublisher stagedOutputPublisher;

    DiagnosticSomaticMetadataApi(final Run run, final RunApi runApi, final SampleApi sampleApi,
            final StagedOutputPublisher stagedOutputPublisher, final Anonymizer anonymizer) {
        this.runApi = runApi;
        this.sampleApi = sampleApi;
        this.stagedOutputPublisher = stagedOutputPublisher;
        this.run = run;
        this.anonymizer = anonymizer;
    }

    @Override
    public SomaticRunMetadata get() {
        String runBucket = ofNullable(run.getBucket()).orElseThrow();
        RunSet set = run.getSet();
        List<Sample> samplesBySet = sampleApi.list(null, null, null, set.getId(), null, null);

        SingleSampleRunMetadata reference = find(SampleType.REF, samplesBySet).map(referenceSample -> toMetadata(referenceSample,
                run,
                SingleSampleRunMetadata.SampleType.REFERENCE,
                anonymizer))
                .orElseThrow(() -> new IllegalStateException(String.format("No reference sample found in SBP for set [%s]",
                        set.getName())));
        String ini = run.getIni();
        if (Ini.SINGLESAMPLE_INI.getValue().equals(ini)) {
            LOGGER.info("Somatic run is using single sample configuration. No algorithms will be run, just transfer and cleanup");
            return SomaticRunMetadata.builder().bucket(runBucket).set(set.getName()).reference(reference).build();
        } else {
            SingleSampleRunMetadata tumor = find(SampleType.TUMOR, samplesBySet).map(referenceSample -> toMetadata(referenceSample,
                    run,
                    SingleSampleRunMetadata.SampleType.TUMOR,
                    anonymizer))
                    .orElseThrow((() -> new IllegalStateException(String.format(
                            "No tumor sample found in SBP for set [%s] and this run was not marked as single sample",
                            set.getName()))));
            return SomaticRunMetadata.builder().bucket(runBucket).set(set.getName()).reference(reference).maybeTumor(tumor).build();
        }
    }

    private static SingleSampleRunMetadata toMetadata(final Sample sample, final Run aRun, final SingleSampleRunMetadata.SampleType type,
            final Anonymizer anonymizer) {
        return SingleSampleRunMetadata.builder()
                .bucket(ofNullable(aRun.getBucket()).orElseThrow())
                .set(aRun.getSet().getName())
                .sampleName(anonymizer.sampleName(sample))
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
                ApiRunStatus.finish(runApi, run, pipelineState.status());
            } catch (Exception e) {
                ApiRunStatus.finish(runApi, run, PipelineStatus.FAILED);
                throw e;
            }
        } else {
            throw new IllegalStateException(String.format(
                    "Run [%s] has no predefined bucket for output. The bucket should be populated externally to P5, check configuration in "
                            + "SBP API.",
                    run.getId()));
        }
    }

    @Override
    public void start() {
        ApiRunStatus.start(runApi, run);
    }
}