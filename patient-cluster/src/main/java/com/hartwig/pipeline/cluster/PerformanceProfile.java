package com.hartwig.pipeline.cluster;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutablePerformanceProfile.class)
public interface PerformanceProfile {

    @Value.Default
    default int workers() {
        return 7;
    }

    @Value.Default
    default int cpuPerNode() {
        return 16;
    }

    @Value.Default
    default int heapPerNodeGB() {
        return 48;
    }

    @Value.Default
    default int offHeapPerNodeGB() {
        return 48;
    }

    @Value.Default
    default int diskSizeGB() {
        return 1000;
    }

    static ImmutablePerformanceProfile.Builder builder() {
        return ImmutablePerformanceProfile.builder();
    }

    static PerformanceProfile defaultProfile() {
        return builder().build();
    }

    static PerformanceProfile from(String path) {
        try {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            return mapper.readValue(new File(path), PerformanceProfile.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
