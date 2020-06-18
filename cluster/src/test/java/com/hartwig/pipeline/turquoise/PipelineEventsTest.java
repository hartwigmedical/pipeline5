package com.hartwig.pipeline.turquoise;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class PipelineEventsTest {

    public static final LocalDateTime NOW = LocalDateTime.of(2020, 6, 19, 0, 0, 0);
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
        victim = PipelineStarted.builder()
                .commonSubjects(common())
                .timestamp(NOW)
                .publisher(publisher)
                .build();
        victim.publish();
        PubsubMessage message = jsonCaptor.getValue();
        assertThat(new String(message.getData().toByteArray())).isEqualTo(
                "{\"timestamp\":[2020,6,19,0,0],\"type\":{\"name\":\"pipeline.started\"},\"subjects\":"
                        + "[{\"name\":\"sample\",\"type\":{\"name\":\"sample\"}}," + "{\"name\":\"set\",\"type\":{\"name\":\"set\"}},"
                        + "{\"name\":\"somatic\",\"type\":{\"name\":\"type\"}},"
                        + "{\"name\":\"ref_barcode\",\"type\":{\"name\":\"barcode\"}},"
                        + "{\"name\":\"tumor_barcode\",\"type\":{\"name\":\"barcode\"}}"
                        + ",{\"name\":\"1\",\"type\":{\"name\":\"run_id\"}}]}");
    }

    @Test
    public void createsEventWithCorrectSubjects() {
        victim = PipelineCompleted.builder()
                .commonSubjects(common())
                .timestamp(NOW)
                .status("Success")
                .publisher(publisher)
                .build();
        victim.publish();
        PubsubMessage message = jsonCaptor.getValue();
        assertThat(new String(message.getData().toByteArray())).isEqualTo(
                "{\"timestamp\":[2020,6,19,0,0],\"type\":{\"name\":\"pipeline.completed\"},\"subjects\":"
                        + "[{\"name\":\"sample\",\"type\":{\"name\":\"sample\"}}," + "{\"name\":\"set\",\"type\":{\"name\":\"set\"}},"
                        + "{\"name\":\"somatic\",\"type\":{\"name\":\"type\"}},"
                        + "{\"name\":\"ref_barcode\",\"type\":{\"name\":\"barcode\"}},"
                        + "{\"name\":\"tumor_barcode\",\"type\":{\"name\":\"barcode\"}}"
                        + ",{\"name\":\"1\",\"type\":{\"name\":\"run_id\"}},{\"name\":\"Success\",\"type\":{\"name\":\"status\"}}]}");
    }

    private static ImmutablePipelineSubjects common() {
        return PipelineSubjects.builder()
                .type("somatic")
                .sample("sample")
                .set("set")
                .referenceBarcode("ref_barcode")
                .tumorBarcode("tumor_barcode")
                .runId(1)
                .build();
    }
}