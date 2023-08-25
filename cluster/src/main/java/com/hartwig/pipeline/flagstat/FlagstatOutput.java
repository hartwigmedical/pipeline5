package com.hartwig.pipeline.flagstat;

import com.hartwig.computeengine.execution.ComputeEngineStatus;
import com.hartwig.computeengine.storage.GoogleStorageLocation;
import com.hartwig.pipeline.StageOutput;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
public interface FlagstatOutput extends StageOutput {

    String sample();

    ComputeEngineStatus status();

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
