package com.hartwig.pipeline.smoke;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalOverrides {
    private final static Logger LOGGER = LoggerFactory.getLogger(LocalOverrides.class);
    private Map<String, String> overrides;

    LocalOverrides() {
        overrides = new HashMap<>();
        File rc = new File(String.format("%s/.hmfrc", System.getProperty("user.home")));
        if (rc.exists()) {
            LOGGER.info("Overrides from [{}] found", rc.getAbsolutePath());
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            try {
                Overrides yaml = mapper.readValue(rc, Overrides.class);
                overrides.put("version", yaml.version());
                overrides.put("keystore", yaml.keystore());
                overrides.put("privateKey", yaml.privateKey());
                overrides.put("rclonePath", yaml.rclonePath());
            } catch (IOException e) {
                LOGGER.warn("Could not parse local overrides", e);
            }
        } else {
            LOGGER.info("No local overrides will be loaded for smoke test");
        }
    }

    public String get(String key, String defaultValue) {
        return overrides.getOrDefault(key, defaultValue);
    }

    @Value.Immutable
    @JsonDeserialize(as = ImmutableOverrides.class)
    public interface Overrides {
        String version();

        String keystore();

        String privateKey();

        String rclonePath();
    }
}
