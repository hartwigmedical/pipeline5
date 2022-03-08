package com.hartwig.batch.input;

import static java.lang.String.format;

import java.io.File;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.hartwig.pipeline.execution.vm.VmDirectories;

import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableInputFileDescriptor.class)
public abstract class InputFileDescriptor {
    @Value.Parameter
    @JsonProperty("key")
    public abstract String name();

    @Value.Parameter
    @JsonProperty("value")
    public abstract String inputValue();

    @Value.Parameter
    private String protocol() {
        return "gs://";
    }

    @Value.Parameter
    public abstract String billedProject();

    public String copyToLocalDestinationCommand() {
        return toCommandForm(localDestination());
    }

    public String toCommandForm(final String localDestination) {
        return String.format(
                "gsutil -o 'GSUtil:parallel_thread_count=1' -o \"GSUtil:sliced_object_download_max_components=$(nproc)\" -q -u %s cp %s%s %s",
                billedProject(),
                protocol(),
                inputValue().replaceAll("^gs://", ""),
                localDestination);
    }

    static ImmutableInputFileDescriptor.Builder builder() {
        return ImmutableInputFileDescriptor.builder();
    }

    public static ImmutableInputFileDescriptor from(final InputFileDescriptor original) {
        return builder().from(original).build();
    }

    public InputFileDescriptor index() {
        if (inputValue().endsWith(".bam")) {
            return index(".bai");
        }

        if (inputValue().endsWith(".cram")) {
            return index(".crai");
        }

        if (inputValue().endsWith(".vcf.gz")) {
            return index(".tbi");
        }

        throw new IllegalArgumentException("Unknown file format");
    }

    public InputFileDescriptor index(final String suffix) {
        return builder().from(this).inputValue(inputValue() + suffix).build();
    }

    public String localDestination() {
        return format("%s/%s", VmDirectories.INPUT, new File(inputValue()).getName());
    }
}
