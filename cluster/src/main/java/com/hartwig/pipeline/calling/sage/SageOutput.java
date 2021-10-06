package com.hartwig.pipeline.calling.sage;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.storage.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface SageOutput extends StageOutput {

    PipelineStatus status();

    Optional<GoogleStorageLocation> maybeFinalVcf();

    Optional <GoogleStorageLocation> maybeGermlineGeneCoverageTsv();
    Optional <GoogleStorageLocation> maybeSomaticRefSampleBqrPlot();
    Optional <GoogleStorageLocation> maybeSomaticTumorSampleBqrPlot();

    default GoogleStorageLocation finalVcf() {
        return maybeFinalVcf().orElseThrow(() -> new IllegalStateException("No final vcf available"));
    }
    default GoogleStorageLocation germlineGeneCoverageTsv() {
        return maybeGermlineGeneCoverageTsv().orElseThrow(() -> new IllegalStateException("No germline gene coverage tsv available"));
    }
    default GoogleStorageLocation somaticRefSampleBqrPlot() {
        return maybeSomaticRefSampleBqrPlot().orElseThrow(() -> new IllegalStateException("No somatic ref sample bqr plot available"));
    }
    default GoogleStorageLocation somaticTumorSampleBqrPlot() {
        return maybeSomaticTumorSampleBqrPlot().orElseThrow(() -> new IllegalStateException("No somatic tumor sample bqr plot available"));
    }

    static ImmutableSageOutput.Builder builder(String nameSpace) {
        return ImmutableSageOutput.builder().name(nameSpace);
    }
}
