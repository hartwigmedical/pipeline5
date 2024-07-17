package com.hartwig.pipeline.turquoise;

import com.google.cloud.pubsub.v1.Publisher;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.input.SomaticRunMetadata;

public interface Turquoise extends AutoCloseable {
    void publishStarted();

    void publishComplete(final String status);

    static Turquoise create(final Publisher publisher, final Arguments arguments, final SomaticRunMetadata metadata) {
        if (arguments.publishToTurquoise()) {
            return new PublishingTurquoise(publisher, arguments, metadata);
        }
        return new Turquoise() {

            @Override
            public void publishStarted() {
                // noop
            }

            @Override
            public void publishComplete(final String status) {
                // noop
            }

            @Override
            public void close() {
                publisher.shutdown();
            }
        };
    }
}
