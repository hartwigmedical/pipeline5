package com.hartwig.pipeline.runtime.configuration;

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class YAMLConfigurationReader {

    public static Configuration from(String workingDirectory) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(new File(workingDirectory + File.separator + "conf" + File.separator + "pipeline.yaml"),
                Configuration.class);
    }
}
