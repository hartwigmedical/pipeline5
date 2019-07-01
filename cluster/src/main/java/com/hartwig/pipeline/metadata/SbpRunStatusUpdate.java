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

    @Value.Parameter
    String bucketName();

    @Value.Default
    default String endTime() {
        return nowUtc().format(DateTimeFormatter.ISO_DATE_TIME);
    }

    @Value.Default
    default String cluster(){
        return "object02";
    }

    static LocalDateTime nowUtc() {
        return LocalDateTime.now(ZoneId.of("UTC"));
    }

    static SbpRunStatusUpdate of (String status, String bucketName){
        return ImmutableSbpRunStatusUpdate.of(status, bucketName);
    }
}
