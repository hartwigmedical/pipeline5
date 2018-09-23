package com.hartwig.pipeline.bootstrap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeInitialization {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeInitialization.class);
    private final String initScriptPath;

    NodeInitialization(final String initScriptPath) {
        this.initScriptPath = initScriptPath;
    }

    public String run(RuntimeBucket bucket) throws FileNotFoundException {
        String initDirectory = "node-init";
        File initScript = new File(initScriptPath);
        LOGGER.info("Uploading node initialization script from [{}] into [gs://{}/{}]", initScriptPath, bucket.getName(), initDirectory);
        String blobPath = String.format("%s/%s", initDirectory, initScript.getName());
        bucket.bucket().create(blobPath, new FileInputStream(initScript));
        return String.format("gs://%s/%s", bucket.getName(), blobPath);
    }
}
