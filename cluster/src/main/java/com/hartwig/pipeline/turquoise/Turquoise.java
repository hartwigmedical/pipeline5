package com.hartwig.pipeline.turquoise;

import com.google.cloud.pubsub.v1.Publisher;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.input.SomaticRunMetadata;

public class Turquoise {

    private final Publisher turquoisePublisher;
    private final Arguments arguments;
    private final SomaticRunMetadata somaticRunMetadata;

    public Turquoise(final Publisher turquoisePublisher, final Arguments arguments, final SomaticRunMetadata somaticRunMetadata) {
        this.turquoisePublisher = turquoisePublisher;
        this.arguments = arguments;
        this.somaticRunMetadata = somaticRunMetadata;
    }

    public void publishStarted() {
        if (arguments.publishToTurquoise()) {
            startedEvent(PipelineProperties.builder()
                    .sample(collectTurquoiseSubject(somaticRunMetadata))
                    .runId(arguments.sbpApiRunId())
                    .set(somaticRunMetadata.set())
                    .referenceBarcode(somaticRunMetadata.maybeReference().map(SingleSampleRunMetadata::barcode))
                    .tumorBarcode(somaticRunMetadata.maybeTumor().map(SingleSampleRunMetadata::barcode))
                    .type(getType())
                    .build(), turquoisePublisher, arguments.publishToTurquoise());
        }
    }

    public void publishComplete(final String status) {
        if (arguments.publishToTurquoise()) {
            completedEvent(PipelineProperties.builder()
                    .sample(collectTurquoiseSubject(somaticRunMetadata))
                    .runId(arguments.sbpApiRunId())
                    .set(somaticRunMetadata.set())
                    .referenceBarcode(somaticRunMetadata.maybeReference().map(SingleSampleRunMetadata::barcode))
                    .tumorBarcode(somaticRunMetadata.maybeTumor().map(SingleSampleRunMetadata::barcode))
                    .type(getType())
                    .build(), turquoisePublisher, status, arguments.publishToTurquoise());
        }
    }

    public static void publish(final TurquoiseEvent turquoiseEvent, final boolean publish) {
        if (publish) {
            turquoiseEvent.publish();
        }
    }

    private static void completedEvent(final PipelineProperties properties, final Publisher publisher, final String status,
            final boolean publish) {
        publish(PipelineCompleted.builder().properties(properties).publisher(publisher).status(status).build(), publish);
    }

    private static void startedEvent(final PipelineProperties subjects, final Publisher publisher, final boolean publish) {
        publish(PipelineStarted.builder().properties(subjects).publisher(publisher).build(), publish);
    }

    private String collectTurquoiseSubject(final SomaticRunMetadata somaticRunMetadata) {
        return somaticRunMetadata.maybeTumor()
                .map(SingleSampleRunMetadata::turquoiseSubject)
                .orElse(somaticRunMetadata.reference().turquoiseSubject())
                .orElseThrow(() -> new IllegalArgumentException("Turquoise is enabled but no turquoise subject is specified in the PDL. "
                        + "You can disable turquoise with the -publish_to_turquoise arg, or add turquoiseSubject to the sample input."));
    }

    private String getType() {
        return somaticRunMetadata.isSingleSample() ? "single_sample" : arguments.shallow() ? "shallow" : "somatic";
    }
}
