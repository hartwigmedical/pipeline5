package com.hartwig.pipeline.sbpapi;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

public class ObjectMappers {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        OBJECT_MAPPER.registerModule(new Jdk8Module());
    }

    public static ObjectMapper get(){
        return OBJECT_MAPPER;
    }
}