package com.hartwig.pipeline.pubsub;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.ProjectTopicName;
import com.hartwig.pipeline.CommonArguments;

public class PublisherProvider {

    private final CommonArguments arguments;
    private final GoogleCredentials credentials;

    private PublisherProvider(final CommonArguments arguments, final GoogleCredentials credentials) {
        this.arguments = arguments;
        this.credentials = credentials;
    }

    public Publisher get(final String topic) throws Exception {
        return Publisher.newBuilder(ProjectTopicName.of(arguments.pubsubProject().orElse(arguments.project()), topic))
                .setCredentialsProvider(() -> credentials)
                .build();
    }

    public static PublisherProvider from(final CommonArguments arguments, final GoogleCredentials credentials) {
        return new PublisherProvider(arguments, credentials);
    }
}
