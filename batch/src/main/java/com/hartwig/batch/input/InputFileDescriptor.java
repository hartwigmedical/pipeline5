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
    public abstract String value();

    @Value.Parameter
    private String protocol() {
        return "gs://";
    }

    @Value.Parameter
    public abstract String billedProject();

    public String toCommandForm(String localDestination) {
        return String.format("gsutil -o 'GSUtil:parallel_thread_count=1' -o \"GSUtil:sliced_object_download_max_components=$(nproc)\" -q -u %s cp %s%s %s",
                billedProject(),
                protocol(),
                value().replaceAll("^gs://", ""),
                localDestination);
    }

    static ImmutableInputFileDescriptor.Builder builder() {
        return ImmutableInputFileDescriptor.builder();
    }

    public static ImmutableInputFileDescriptor from(InputFileDescriptor original) {
        return builder().from(original).build();
    }
}
