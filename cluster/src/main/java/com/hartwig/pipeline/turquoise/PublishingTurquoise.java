package com.hartwig.pipeline.turquoise;

import com.google.cloud.pubsub.v1.Publisher;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.input.SingleSampleRunMetadata;
import com.hartwig.pipeline.input.SomaticRunMetadata;

public class PublishingTurquoise implements Turquoise {

    private final Publisher turquoisePublisher;
    private final SomaticRunMetadata somaticRunMetadata;
    private final PipelineProperties properties;

    public PublishingTurquoise(final Publisher turquoisePublisher, final Arguments arguments, final SomaticRunMetadata somaticRunMetadata) {
        this.turquoisePublisher = turquoisePublisher;
        this.somaticRunMetadata = somaticRunMetadata;
        this.properties = PipelineProperties.builder()
                .sample(collectTurquoiseSubject(somaticRunMetadata))
                .runId(arguments.sbpApiRunId())
                .set(somaticRunMetadata.set())
                .referenceBarcode(somaticRunMetadata.maybeReference().map(SingleSampleRunMetadata::barcode))
                .tumorBarcode(somaticRunMetadata.maybeTumor().map(SingleSampleRunMetadata::barcode))
                .type(getType(arguments.shallow()))
                .build();
    }

    @Override
    public void publishStarted() {
        startedEvent(properties, turquoisePublisher);
    }

    @Override
    public void publishComplete(final String status) {
        completedEvent(properties, turquoisePublisher, status);
    }

    @Override
    public void close() {
        turquoisePublisher.shutdown();
    }

    private static void completedEvent(final PipelineProperties subjects, final Publisher publisher, final String status) {
        PipelineCompleted.builder().properties(subjects).publisher(publisher).status(status).build().publish();
    }

    private static void startedEvent(final PipelineProperties subjects, final Publisher publisher) {
        PipelineStarted.builder().properties(subjects).publisher(publisher).build().publish();
    }

    private String collectTurquoiseSubject(final SomaticRunMetadata somaticRunMetadata) {
        return somaticRunMetadata.maybeTumor()
                .or(somaticRunMetadata::maybeReference)
                .flatMap(SingleSampleRunMetadata::turquoiseSubject)
                .orElseThrow(() -> new IllegalArgumentException("Turquoise is enabled but no turquoise subject is specified in the PDL. "
                        + "You can disable turquoise with the -publish_to_turquoise arg, or add turquoiseSubject to the sample input."));
    }

    private String getType(boolean shallow) {
        return somaticRunMetadata.isSingleSample() ? "single_sample" : shallow ? "shallow" : "somatic";
    }
}
