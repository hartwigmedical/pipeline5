package com.hartwig.pipeline.turquoise;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.input.ImmutableSingleSampleRunMetadata;
import com.hartwig.pipeline.input.ImmutableSomaticRunMetadata;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.testsupport.TestInputs;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PublishingTurquoiseTest {

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowWhenStartedEventIfTurquoiseEnabledAndNoTurquoiseSubject() {
        Publisher publisher = mock(Publisher.class);
        PublishingTurquoise victim = new PublishingTurquoise(publisher,
                Arguments.testDefaultsBuilder().publishToTurquoise(true).build(),
                ImmutableSomaticRunMetadata.builder()
                        .from(TestInputs.defaultSomaticRunMetadata())
                        .maybeTumor(ImmutableSingleSampleRunMetadata.builder()
                                .from(TestInputs.tumorRunMetadata())
                                .turquoiseSubject(Optional.empty())
                                .build())
                        .build());
        victim.publishStarted();
    }

    @Test
    public void shouldPublishWhenStartedEventAndTurquoiseSubject() throws Exception {
        Publisher publisher = mock(Publisher.class);
        ArgumentCaptor<PubsubMessage> messageCaptor = setupPublisher(publisher);
        PublishingTurquoise victim = new PublishingTurquoise(publisher,
                Arguments.testDefaultsBuilder().publishToTurquoise(true).build(),
                TestInputs.defaultSomaticRunMetadata());
        victim.publishStarted();
        Event event = assertEvent(messageCaptor);
        assertThat(event.labels()).containsExactly(Label.of("type", "somatic"));
        assertThat(event.type()).isEqualTo("pipeline.started");
    }

    @Test
    public void shouldPublishWhenCompletedEventAndTurquoiseSubject() throws Exception {
        Publisher publisher = mock(Publisher.class);
        ArgumentCaptor<PubsubMessage> messageCaptor = setupPublisher(publisher);
        PublishingTurquoise victim = new PublishingTurquoise(publisher,
                Arguments.testDefaultsBuilder().publishToTurquoise(true).build(),
                TestInputs.defaultSomaticRunMetadata());
        victim.publishComplete(PipelineStatus.SUCCESS.toString());
        Event event = assertEvent(messageCaptor);
        assertThat(event.labels()).containsExactly(Label.of("type", "somatic"), Label.of("status", "SUCCESS"));
        assertThat(event.type()).isEqualTo("pipeline.completed");
    }

    private static Event assertEvent(final ArgumentCaptor<PubsubMessage> messageCaptor) throws JsonProcessingException {
        Event event = ObjectMappers.get().readValue(new String(messageCaptor.getValue().getData().toByteArray()), Event.class);
        assertThat(event.subjects()).containsExactly(Subject.of("sample",
                "tumor",
                List.of(Label.of("set", "set"), Label.of("barcode", "tumor"))));
        return event;
    }

    @NotNull
    private static ArgumentCaptor<PubsubMessage> setupPublisher(final Publisher publisher)
            throws InterruptedException, ExecutionException, TimeoutException {
        ArgumentCaptor<PubsubMessage> messageCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        @SuppressWarnings("unchecked")
        ApiFuture<String> mock = mock(ApiFuture.class);
        when(mock.get(10, TimeUnit.SECONDS)).thenReturn("");
        when(publisher.publish(messageCaptor.capture())).thenReturn(mock);
        return messageCaptor;
    }
}