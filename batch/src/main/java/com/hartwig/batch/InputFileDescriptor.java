package com.hartwig.batch;

import org.immutables.value.Value;

@Value.Immutable
public abstract class InputFileDescriptor {
    @Value.Parameter
    public abstract String remoteFilename();

    @Value.Parameter
    public String protocol() {
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
}
