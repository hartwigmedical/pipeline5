package com.hartwig.pipeline.calling.structural;

import java.util.Optional;

import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.execution.JobStatus;
import com.hartwig.pipeline.io.GoogleStorageLocation;

import org.immutables.value.Value;

@Value.Immutable
public interface StructuralCallerOutput extends StageOutput {

    @Override
    default String name() {
        return "structural_caller";
    }

    Optional<GoogleStorageLocation> maybeStructuralVcf();

    Optional<GoogleStorageLocation> maybeSvRecoveryVcf();

    default GoogleStorageLocation structuralVcf(){
        return maybeStructuralVcf().orElseThrow(() -> new IllegalStateException("No structural VCF available"));
    }

    default GoogleStorageLocation svRecoveryVcf() {
        return maybeSvRecoveryVcf().orElseThrow(() -> new IllegalStateException("No sv recovery VCF available"));
    }

    static ImmutableStructuralCallerOutput.Builder builder() {
        return ImmutableStructuralCallerOutput.builder();
    }
}
