package com.hartwig.pipeline.aqua;

import java.io.IOException;

import com.hartwig.events.aqua.model.AquaEvent;
import com.hartwig.events.pubsub.PubsubEventBuilder;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.tools.VersionUtils;

import org.jetbrains.annotations.NotNull;

public interface AquaPublisher {
    void publishStarted();

    void publishComplete(String status);

    static AquaPublisher create(Arguments arguments, SomaticRunMetadata somaticRunMetadata) throws IOException {
        if (!arguments.publishToAqua()) {
            return noopAquaPublisher();
        }
        var project = arguments.aquaProject().orElseThrow();
        var barcode = somaticRunMetadata.barcode();
        var shallow = arguments.shallow();
        var targeted = arguments.useTargetRegions();
        var pipelineContext = arguments.context();
        var version = VersionUtils.pipelineVersion();
        var publisher = new PubsubEventBuilder().newPublisher(project, new AquaEvent.EventDescriptor());
        return new AquaPublisherImpl(publisher, barcode, shallow, targeted, pipelineContext, version);
    }

    @NotNull
    private static AquaPublisher noopAquaPublisher() {
        return new AquaPublisher() {
            @Override
            public void publishStarted() {
            }

            @Override
            public void publishComplete(String status) {
            }
        };
    }
}
