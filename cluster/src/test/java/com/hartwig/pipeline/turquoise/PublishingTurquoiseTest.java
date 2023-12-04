package com.hartwig.pipeline.turquoise;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.input.ImmutableSingleSampleRunMetadata;
import com.hartwig.pipeline.input.ImmutableSomaticRunMetadata;
import com.hartwig.pipeline.testsupport.TestInputs;

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
        ArgumentCaptor<PubsubMessage> messageCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        @SuppressWarnings("unchecked")
        ApiFuture<String> mock = mock(ApiFuture.class);
        when(mock.get(10, TimeUnit.SECONDS)).thenReturn("");
        when(publisher.publish(messageCaptor.capture())).thenReturn(mock);
        PublishingTurquoise victim = new PublishingTurquoise(publisher,
                Arguments.testDefaultsBuilder().publishToTurquoise(true).build(),
                TestInputs.defaultSomaticRunMetadata());
        victim.publishStarted();

        assertThat(new String(messageCaptor.getValue().getData().toByteArray())).matches(string -> string.endsWith(
                "\"type\":\"pipeline.started\",\"subjects\":[{\"name\":\"tumor\",\"type\":\"sample\",\"labels\":[{\"name\":\"set\",\"value\":\"set\"},"
                        + "{\"name\":\"barcode\",\"value\":\"tumor\"}]}],\"labels\":[{\"name\":\"type\",\"value\":\"somatic\"}]}"));
    }
}