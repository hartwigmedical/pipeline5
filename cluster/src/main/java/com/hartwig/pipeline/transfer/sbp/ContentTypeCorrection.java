package com.hartwig.pipeline.transfer.sbp;

import java.util.Map;

import com.google.cloud.storage.Blob;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentTypeCorrection {

    private static final Logger LOGGER = LoggerFactory.getLogger(ContentTypeCorrection.class);

    private final Map<String, String> KNOWN_TYPES = ImmutableMap.of("txt", "text/plain", "conf", "text/plain");

    public void apply(final Blob blob) {
        String[] split = blob.getName().split("\\.");
        String extension = split[split.length - 1];
        if (KNOWN_TYPES.containsKey(extension) && blob.getContentType() == null) {
            String corrected = KNOWN_TYPES.get(extension);
            LOGGER.info("Found a blob [{}] with extension [{}] with no content type. Correcting with content type [{}] for rclone",
                    blob.getName(),
                    extension,
                    corrected);
            blob.toBuilder().setContentType(corrected).build().update();
        }
    }

    static ContentTypeCorrection get() {
        return new ContentTypeCorrection();
    }
}
