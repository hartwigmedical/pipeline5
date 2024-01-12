package com.hartwig.pipeline.aqua;

import java.io.IOException;
import java.time.Instant;

import com.hartwig.events.EventBuilder;
import com.hartwig.events.EventPublisher;
import com.hartwig.events.aqua.ImmutableMolecularPipelineStartedEvent;
import com.hartwig.events.aqua.MolecularPipelineCompletedEvent;
import com.hartwig.events.aqua.MolecularPipelineStartedEvent;
import com.hartwig.events.aqua.model.AquaEvent;
import com.hartwig.events.pipeline.Pipeline;
import com.hartwig.events.pubsub.PubsubEventBuilder;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.tools.VersionUtils;

public class AquaPublisher {
    private final EventPublisher<AquaEvent> publisher;
    private final String barcode;
    private final boolean isShallow;
    private final boolean isTargeted;
    private final Pipeline.Context pipelineContext;
    private final String version;

    public AquaPublisher(final EventPublisher<AquaEvent> publisher, final String barcode, final boolean isShallow, final boolean isTargeted,
            final Pipeline.Context pipelineContext, final String version) {
        this.publisher = publisher;
        this.barcode = barcode;
        this.isShallow = isShallow;
        this.isTargeted = isTargeted;
        this.pipelineContext = pipelineContext;
        this.version = version;
    }

    public void publishStarted() {
        var startedEvent = MolecularPipelineStartedEvent.builder()
                .timestamp(Instant.now())
                .barcode(barcode)
                .shallow(isShallow)
                .targeted(isTargeted)
                .pipelineContext(pipelineContext)
                .version(version)
                .build();
        publisher.publish(startedEvent);
    }

    public void publishComplete() {
        var completedEvent = MolecularPipelineCompletedEvent.builder()
                .timestamp(Instant.now())
                .barcode(barcode)
                .shallow(isShallow)
                .targeted(isTargeted)
                .pipelineContext(pipelineContext)
                .version(version)
                .build();
        publisher.publish(completedEvent);
    }

    public static AquaPublisher create(Arguments arguments, SomaticRunMetadata somaticRunMetadata) throws IOException {
        var project = arguments.aquaProject();
        var barcode = somaticRunMetadata.barcode();
        var shallow = arguments.shallow();
        var targeted = arguments.useTargetRegions();
        var pipelineContext = arguments.context();
        var version = VersionUtils.pipelineVersion();
        var publisher = new PubsubEventBuilder().newPublisher(project, new AquaEvent.EventDescriptor());
        return new AquaPublisher(publisher, barcode, shallow, targeted, pipelineContext, version);
    }
}
