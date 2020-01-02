package com.hartwig.batch.input;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableInputFileDescriptor.class)
public abstract class InputFileDescriptor {
    @Value.Parameter
    @JsonProperty("key")
    public abstract String name();

    @Value.Parameter
    @JsonProperty("value")
    public abstract String remoteFilename();

    @Value.Parameter
    private String protocol() {
        return "gs://";
    }

    @Value.Parameter
    public abstract String billedProject();

    public String toCommandForm(String localDestination) {
        return String.format("gsutil -q -u %s cp %s%s %s",
                billedProject(),
                protocol(),
                remoteFilename().replaceAll("^gs://", ""),
                localDestination);
    }

    static ImmutableInputFileDescriptor.Builder builder() {
        return ImmutableInputFileDescriptor.builder();
    }

    public static ImmutableInputFileDescriptor from(InputFileDescriptor original) {
        return builder().from(original).build();
    }
}
