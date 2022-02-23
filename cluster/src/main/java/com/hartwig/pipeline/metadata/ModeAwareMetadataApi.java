package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.PipelineState;

public class ModeAwareMetadataApi implements SomaticMetadataApi {

    private final SomaticMetadataApi decorated;
    private final ModeResolver resolver;

    private ModeAwareMetadataApi(final SomaticMetadataApi decorated, final ModeResolver resolver) {
        this.decorated = decorated;
        this.resolver = resolver;
    }

    public static ModeAwareMetadataApi of(final SomaticMetadataApi decorated) {
        return new ModeAwareMetadataApi(decorated, new ModeResolver());
    }

    @Override
    public SomaticRunMetadata get() {
        final SomaticRunMetadata metadata = decorated.get();
        return ImmutableSomaticRunMetadata.builder().from(metadata).mode(resolver.apply(metadata)).build();
    }

    @Override
    public void start() {
        decorated.start();
    }

    @Override
    public void complete(final PipelineState state, final SomaticRunMetadata metadata) {
        decorated.complete(state, metadata);
    }
}
