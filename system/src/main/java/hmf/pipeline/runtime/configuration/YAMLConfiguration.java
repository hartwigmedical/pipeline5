package hmf.pipeline.runtime.configuration;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import hmf.pipeline.Configuration;

public class YAMLConfiguration {

    public static Configuration from(String workingDirectory) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Map yamlMap = mapper.readValue(new File(workingDirectory + File.separator + "conf" + File.separator + "pipeline.yaml"), Map.class);
        return Configuration.builder()
                .patientName(extractSubProperty("patient", "name", yamlMap))
                .patientDirectory(extractSubProperty("patient", "directory", yamlMap))
                .referencePath(extractSubProperty("patient", "referencePath", yamlMap))
                .sparkMaster(extractSubProperty("spark", "master", yamlMap))
                .build();
    }

    private static String extractSubProperty(final String property, final String subProperty, final Map configurationMap) {
        return (String) ((Map) configurationMap.get(property)).get(subProperty);
    }
}
