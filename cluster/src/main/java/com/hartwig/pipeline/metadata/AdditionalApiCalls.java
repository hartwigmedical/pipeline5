package com.hartwig.pipeline.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.hartwig.pipeline.sbpapi.FileResponse;
import com.hartwig.pipeline.sbpapi.SbpRestApi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdditionalApiCalls {
    private static final AdditionalApiCalls INSTANCE = new AdditionalApiCalls();
    private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalApiCalls.class);
    private Map<String, List<ApiFileOperation>> metadata;

    private AdditionalApiCalls() {
        metadata = new HashMap<>();
        //String name = RunTag.apply(arguments, metadata.sampleId());
    }

    public static AdditionalApiCalls instance() {
        return INSTANCE;
    }

    public synchronized void register(String filePath, ApiFileOperation extraOperation) {
        metadata.putIfAbsent(filePath, new ArrayList<>());
        metadata.get(filePath).add(extraOperation);
        LOGGER.info("Registered [{}] to file path [{}]", extraOperation, filePath);
    }

    public void apply(final SbpRestApi sbpApi, String filePath, FileResponse fileResponse) {
        LOGGER.info("Applying additional operations for path [{}]", filePath);

        // Replace this with passing in the name to the constructor?
        String prefixRemoved = filePath.substring(filePath.indexOf("/") + 1);
        List<ApiFileOperation> extraOps = metadata.get(prefixRemoved);
        if (extraOps != null) {
            extraOps.forEach(o -> {
                LOGGER.info("Found operation [{}] to apply", o);
                o.apply(sbpApi, fileResponse);
            });
        }
    }
}
