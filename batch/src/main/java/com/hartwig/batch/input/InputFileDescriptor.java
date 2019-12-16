package com.hartwig.batch.input;

import org.immutables.value.Value;

@Value.Immutable
public abstract class InputFileDescriptor {
    @Value.Parameter
    public abstract String name();

    @Value.Parameter
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
