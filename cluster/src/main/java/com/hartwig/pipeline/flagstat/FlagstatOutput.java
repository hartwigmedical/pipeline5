package com.hartwig.pipeline.flagstat;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface FlagstatOutput extends StageOutput {

    String sample();

    PipelineStatus status();

    @Override
    default String name() {
        return Flagstat.NAMESPACE;
    }

    Optional<GoogleStorageLocation> maybeFlagstatOutputFile();

    default GoogleStorageLocation flagstatOutputFile() {
        return maybeFlagstatOutputFile().orElse(GoogleStorageLocation.empty());
    }

    static String outputFile(final String sample) {
        return String.format("%s.flagstat", sample);
    }

    static ImmutableFlagstatOutput.Builder builder() {
        return ImmutableFlagstatOutput.builder();
    }
}
