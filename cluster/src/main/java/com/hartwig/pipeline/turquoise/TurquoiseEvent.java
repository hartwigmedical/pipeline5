package com.hartwig.pipeline.turquoise;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.hartwig.pipeline.jackson.ObjectMappers;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface TurquoiseEvent {

    Logger LOGGER = LoggerFactory.getLogger(TurquoiseEvent.class);

    String eventType();

    List<Subject> subjects();

    @Value.Default
    default LocalDateTime timestamp() {
        return LocalDateTime.now();
    }

    Publisher publisher();

    default void publish() {
        try {
            LOGGER.info("Publishing message to Turquoise [{}]", this);
            ApiFuture<String> future = publisher().publish(PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(ObjectMappers.get()
                            .writeValueAsString(Event.of(timestamp(), eventType(), subjects()))))
                    .build());
            LOGGER.info("Message was published with id [{}]", future.get(10, TimeUnit.SECONDS));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}