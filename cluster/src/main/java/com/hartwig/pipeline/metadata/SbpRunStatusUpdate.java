package com.hartwig.pipeline.metadata;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import org.immutables.value.Value;

@JsonSerialize(as = ImmutableSbpRunStatusUpdate.class)
@Value.Immutable
public interface SbpRunStatusUpdate {

    @Value.Parameter
    String status();

    @Value.Default
    default String endTime() {
        return nowUtc().format(DateTimeFormatter.ISO_DATE_TIME);
    }

    @Value.Default
    default String cluster(){
        return "object02";
    }

    @Value.Default
    default String bucket() {
        return "hmf-output-" + nowUtc().format(DateTimeFormatter.ofPattern("yyyy-dd"));
    }

    static LocalDateTime nowUtc() {
        return LocalDateTime.now(ZoneId.of("UTC"));
    }

    static SbpRunStatusUpdate of (String status){
        return ImmutableSbpRunStatusUpdate.of(status);
    }
}
