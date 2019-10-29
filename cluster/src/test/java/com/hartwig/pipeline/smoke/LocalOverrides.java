package com.hartwig.pipeline.smoke;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

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
                OverridesData yaml = mapper.readValue(rc, OverridesData.class);
                overrides = yaml.toMap();
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

    @JsonDeserialize
    private static class OverridesData {
        String version;
        String keystore;
        String privateKey;
        String rclonePath;

        String getVersion() {
            return version;
        }

        String getKeystore() {
            return keystore;
        }

        String getPrivateKey() {
            return privateKey;
        }

        String getRclonePath() {
            return rclonePath;
        }

        Map<String, String> toMap() {
            Map<String, String> map = new HashMap<>();
            map.put("version", getVersion());
            map.put("keystore", getKeystore());
            map.put("privateKey", getPrivateKey());
            map.put("rclonePath", getRclonePath());
            return map;
        }
    }
}
