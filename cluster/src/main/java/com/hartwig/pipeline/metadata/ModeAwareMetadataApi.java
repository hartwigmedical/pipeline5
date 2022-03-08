package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.PipelineState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ModeAwareMetadataApi implements SomaticMetadataApi {

    private static final Logger LOGGER = LoggerFactory.getLogger(ModeAwareMetadataApi.class);

    private final SomaticMetadataApi decorated;
    private final ModeResolver resolver;
    private InputMode mode;

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
        if (mode == null) {
            mode = resolver.apply(metadata);
            LOGGER.info("Resolved sample input mode [{}]", mode);
        }
        return ImmutableSomaticRunMetadata.builder().from(metadata).mode(mode).build();
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
