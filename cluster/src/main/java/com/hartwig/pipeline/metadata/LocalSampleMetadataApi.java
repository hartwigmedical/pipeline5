package com.hartwig.pipeline.metadata;

import java.util.HashSet;
import java.util.Set;

import com.hartwig.pipeline.PipelineState;

public class LocalSampleMetadataApi implements SampleMetadataApi {

    private final String sampleId;
    private final Set<CompletionHandler> handlers = new HashSet<>();

    public LocalSampleMetadataApi(final String sampleId) {
        this.sampleId = sampleId;
    }

    public SingleSampleRunMetadata get() {
        return SingleSampleRunMetadata.builder()
                .sampleId(sampleId)
                .type(sampleId.toUpperCase().endsWith("T")
                        ? SingleSampleRunMetadata.SampleType.TUMOR
                        : SingleSampleRunMetadata.SampleType.REFERENCE)
                .build();
    }

    public Set<CompletionHandler> getHandlers() {
        return handlers;
    }

    public void register(CompletionHandler completionHandler) {
        handlers.add(completionHandler);
    }

    @Override
    public void alignmentComplete(final PipelineState state) {
        handlers.forEach(handler -> handler.handleAlignmentComplete(state));
    }

    @Override
    public void complete(PipelineState state) {
        handlers.forEach(handler -> handler.handleAlignmentComplete(state));
    }
}