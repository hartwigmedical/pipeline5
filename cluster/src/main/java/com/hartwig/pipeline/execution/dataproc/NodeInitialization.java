package com.hartwig.pipeline.execution.dataproc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import com.hartwig.pipeline.storage.RuntimeBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeInitialization {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeInitialization.class);
    private final String initScriptPath;

    public NodeInitialization(final String initScriptPath) {
        this.initScriptPath = initScriptPath;
    }

    public String run(RuntimeBucket bucket) throws FileNotFoundException {
        String initDirectory = "node-init";
        File initScript = new File(initScriptPath);
        LOGGER.debug("Uploading node initialization script of [{}] into [gs://{}/{}]", initScriptPath, bucket.name(), initDirectory);
        String blobPath = String.format("%s/%s", initDirectory, initScript.getName());
        bucket.create(blobPath, new FileInputStream(initScript));
        return String.format("gs://%s/%s", bucket.name(), blobPath);
    }
}
