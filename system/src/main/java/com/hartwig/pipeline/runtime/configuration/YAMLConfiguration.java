package com.hartwig.pipeline.runtime.configuration;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.hartwig.pipeline.Configuration;

public class YAMLConfiguration {

    private static final String PATIENT_PROPERTY = "patient";
    private static final String SPARK_PROPERTY = "spark";

    public static Configuration from(String workingDirectory) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Map yamlMap = mapper.readValue(new File(workingDirectory + File.separator + "conf" + File.separator + "pipeline.yaml"), Map.class);

        Map<String, String> sparkProperties = extractSubMap(SPARK_PROPERTY, yamlMap);

        return Configuration.builder().flavour(Configuration.Flavour.valueOf(extractSubProperty("pipeline", "flavour", yamlMap)))
                .patientName(extractSubProperty(PATIENT_PROPERTY, "name", yamlMap))
                .patientDirectory(extractSubProperty(PATIENT_PROPERTY, "directory", yamlMap))
                .referencePath(extractSubProperty(PATIENT_PROPERTY, "referencePath", yamlMap))
                .sparkMaster(extractSubProperty(SPARK_PROPERTY, "master", yamlMap)).putAllSparkProperties(sparkProperties)
                .build();
    }

    private static String extractSubProperty(final String property, final String subProperty, final Map configurationMap) {
        return (String) ((Map) configurationMap.get(property)).get(subProperty);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> extractSubMap(final String property, final Map configurationMap) {
        return ((Map) configurationMap.get(property));
    }
}
