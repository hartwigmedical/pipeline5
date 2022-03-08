package com.hartwig.pipeline.turquoise;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.hartwig.pipeline.testsupport.Resources;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PipelineEventsTest {

    public static final ZonedDateTime NOW = ZonedDateTime.of(2020, 6, 19, 0, 0, 0, 0, ZoneId.of("UTC"));
    private Publisher publisher;
    private ArgumentCaptor<PubsubMessage> jsonCaptor;
    private PipelineEvent victim;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        publisher = mock(Publisher.class);
        jsonCaptor = ArgumentCaptor.forClass(PubsubMessage.class);
        ApiFuture<String> apiFuture = mock(ApiFuture.class);
        when(publisher.publish(jsonCaptor.capture())).thenReturn(apiFuture);
        when(apiFuture.get()).thenReturn("messageId");
    }

    @Test
    public void startedEventCreatesEventWithCorrectSubjects() {
        victim = PipelineStarted.builder().properties(common()).timestamp(NOW).publisher(publisher).build();
        victim.publish();
        PubsubMessage message = jsonCaptor.getValue();
        assertThat(new String(message.getData().toByteArray())).isEqualTo(json("pipeline.started"));
    }

    @Test
    public void completedEventCreatesEventWithCorrectSubjects() {
        victim = PipelineCompleted.builder().properties(common()).timestamp(NOW).status("Success").publisher(publisher).build();
        victim.publish();
        PubsubMessage message = jsonCaptor.getValue();
        assertThat(new String(message.getData().toByteArray())).isEqualTo(json("pipeline.completed"));
    }

    private static PipelineProperties common() {
        return PipelineProperties.builder()
                .type("somatic")
                .sample("sample")
                .set("set")
                .referenceBarcode("ref_barcode")
                .tumorBarcode("tumor_barcode")
                .runId(1)
                .build();
    }

    @NotNull
    private static String json(final String name) {
        try {
            return new String(Files.readAllBytes(Paths.get(Resources.testResource("events/" + name + ".json")))).replace("\n", "")
                    .replace(" ", "");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}