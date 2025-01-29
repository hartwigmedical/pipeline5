package com.hartwig.pipeline.input;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.hartwig.pdl.OperationalReferences;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSomaticRunMetadata.class)
@Value.Immutable
public interface SomaticRunMetadata extends RunMetadata {

    // The run name is used downstream to construct compute instance names (which are limited by GCP to max 63 characters)
    // We prepend the run name with "run" (length 3) and append with stage name (longest stage is "orange_no_germline", length 18)
    // So the current maximum for run name in production environment is 63 minus (3+1) minus (1+18) = 40
    int MAX_RUN_NAME_LENGTH = 36;

    String SOMATIC_STAGE_PREFIX = "som";

    @JsonProperty("external_ids")
    Optional<OperationalReferences> maybeExternalIds();

    @JsonProperty("reference")
    Optional<SingleSampleRunMetadata> maybeReference();

    @JsonProperty("tumor")
    Optional<SingleSampleRunMetadata> maybeTumor();

    @Override
    default String runName() {
        if (maybeTumor().isPresent()) {
            return truncate(tumor().barcode());
        } else if (maybeReference().isPresent()) {
            return truncate(reference().barcode());
        } else {
            throw new IllegalStateException("Neither tumor nor reference is present in this run/set");
        }
    }

    @Override
    default String barcode() {
        return maybeTumor().map(SingleSampleRunMetadata::barcode).orElseGet(() -> reference().barcode());
    }

    default String sampleName() {
        return maybeTumor().map(SingleSampleRunMetadata::sampleName).orElseGet(() -> reference().sampleName());
    }

    static String truncate(final String run) {
        return run.length() > MAX_RUN_NAME_LENGTH ? run.substring(0, MAX_RUN_NAME_LENGTH) : run;
    }

    @JsonIgnore
    @Value.Derived
    default boolean isSingleSample() {
        return maybeTumor().map(s -> Boolean.FALSE).orElse(Boolean.TRUE);
    }

    default SingleSampleRunMetadata tumor() {
        return maybeTumor().orElseThrow(() -> new IllegalStateException(
                "No tumor is present in this run/set - somatic routines should not be called"));
    }

    default SingleSampleRunMetadata reference() {
        return maybeReference().orElseThrow(() -> new IllegalStateException(
                "No reference is present in this run/set - germline routines should not be called"));
    }

    static ImmutableSomaticRunMetadata.Builder builder() {
        return ImmutableSomaticRunMetadata.builder();
    }

    @Override
    default String stagePrefix() {
        return SOMATIC_STAGE_PREFIX;
    }
}