package com.hartwig.pipeline.output;

import static java.lang.String.format;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.input.RunMetadata;
import com.hartwig.pipeline.jackson.ObjectMappers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComposeInPipelineOutputBucket implements PipelineOutputComposer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComposeInPipelineOutputBucket.class);

    private final String version;
    private final Storage storage;
    private final Bucket reportBucket;
    private final List<OutputComponent> components = new ArrayList<>();

    ComposeInPipelineOutputBucket(final String version, final Storage storage, final Bucket reportBucket) {
        this.version = version;
        this.storage = storage;
        this.reportBucket = reportBucket;
    }

    public <T extends StageOutput> T add(final T stageOutput) {
        if (stageOutput != null) {
            components.addAll(stageOutput.reportComponents());
        }
        return stageOutput;
    }

    public void compose(final RunMetadata metadata, final Folder folder) {
        String name = metadata.set();
        writeMetadata(metadata, name, folder);
        LOGGER.info("Composing pipeline run results for {} in GCS path gs://{}/{}/{}", name, reportBucket.getName(), name, folder.name());
        reportBucket.create(path(name, folder, "pipeline.version"), version.getBytes());
        try {
            reportBucket.create(path(name, folder, "run.log"), new FileInputStream("run.log"));
        } catch (FileNotFoundException e) {
            LOGGER.warn("No run.log found in working directory. Pipeline logs will not be available in results");
        }
        components.forEach(component -> {
            try {
                component.addToOutput(storage, reportBucket, name);
            } catch (Exception e) {
                throw new RuntimeException(format("Unable add component [%s] to the final patient output.",
                        component.getClass().getSimpleName()), e);
            }
        });
        LOGGER.info("Composition complete for {} in GCS path gs://{}/{}/{}", name, reportBucket.getName(), name, folder.name());
    }

    private void writeMetadata(final Object metadata, final String name, final Folder folder) {
        try {
            reportBucket.create(path(name, folder, "metadata.json"), ObjectMappers.get().writeValueAsBytes(metadata));
        } catch (JsonProcessingException e) {
            LOGGER.warn("Unable to write metadata file for metadata object [{}]", metadata);
        }
    }

    private String path(final String name, final Folder folder, final String fileName) {
        return String.format("%s/%s%s", name, folder.name(), fileName);
    }
}