package com.hartwig.pipeline.calling.sage;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface SageOutput extends StageOutput {

    PipelineStatus status();

    Optional<GoogleStorageLocation> maybeVariants();

    Optional<GoogleStorageLocation> maybeGermlineGeneCoverage();

    Optional<GoogleStorageLocation> maybeSomaticRefSampleBqrPlot();

    Optional<GoogleStorageLocation> maybeSomaticTumorSampleBqrPlot();

    default GoogleStorageLocation variants() { return maybeVariants().orElse(GoogleStorageLocation.empty()); }

    default GoogleStorageLocation germlineGeneCoverage() {
        return maybeGermlineGeneCoverage().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation somaticRefSampleBqrPlot() {
        return maybeSomaticRefSampleBqrPlot().orElse(GoogleStorageLocation.empty());
    }

    default GoogleStorageLocation somaticTumorSampleBqrPlot() {
        return maybeSomaticTumorSampleBqrPlot().orElse(GoogleStorageLocation.empty());
    }

    static ImmutableSageOutput.Builder builder(final String nameSpace) {
        return ImmutableSageOutput.builder().name(nameSpace);
    }
}
