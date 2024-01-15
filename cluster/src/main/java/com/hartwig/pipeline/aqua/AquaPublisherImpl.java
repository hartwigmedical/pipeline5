package com.hartwig.pipeline.aqua;

import java.time.Instant;

import com.hartwig.events.EventPublisher;
import com.hartwig.events.aqua.MolecularPipelineCompletedEvent;
import com.hartwig.events.aqua.MolecularPipelineStartedEvent;
import com.hartwig.events.aqua.model.AquaEvent;
import com.hartwig.events.pipeline.Pipeline;

public class AquaPublisherImpl implements AquaPublisher {
    private final EventPublisher<AquaEvent> publisher;
    private final String barcode;
    private final boolean isShallow;
    private final boolean isTargeted;
    private final Pipeline.Context pipelineContext;
    private final String version;

    public AquaPublisherImpl(final EventPublisher<AquaEvent> publisher, final String barcode, final boolean isShallow,
            final boolean isTargeted, final Pipeline.Context pipelineContext, final String version) {
        this.publisher = publisher;
        this.barcode = barcode;
        this.isShallow = isShallow;
        this.isTargeted = isTargeted;
        this.pipelineContext = pipelineContext;
        this.version = version;
    }

    @Override
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

    @Override
    public void publishComplete(String status) {
        var completedEvent = MolecularPipelineCompletedEvent.builder()
                .timestamp(Instant.now())
                .barcode(barcode)
                .shallow(isShallow)
                .targeted(isTargeted)
                .pipelineContext(pipelineContext)
                .version(version)
                .status(status)
                .build();
        publisher.publish(completedEvent);
    }
}
