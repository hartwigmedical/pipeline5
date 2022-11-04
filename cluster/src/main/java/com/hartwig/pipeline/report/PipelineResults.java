package com.hartwig.pipeline.report;

import static java.lang.String.format;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.metadata.RunMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineResults {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineResults.class);

    private final String version;
    private final Storage storage;
    private final Bucket reportBucket;
    private final boolean isNoOp;
    private final List<ReportComponent> components = new ArrayList<>();

    PipelineResults(final String version, final Storage storage, final Bucket reportBucket, final boolean isNoOp) {
        this.version = version;
        this.storage = storage;
        this.reportBucket = reportBucket;
        this.isNoOp = isNoOp;
    }

    public <T extends StageOutput> T add(final T stageOutput) {
        if (stageOutput != null) {
            components.addAll(stageOutput.reportComponents());
        }
        return stageOutput;
    }

    public void compose(final RunMetadata metadata, final String qualifier) {
        if (!isNoOp) {
            String name = metadata.set();
            Folder folder = Folder.root();
            writeMetadata(metadata, name, folder);
            compose(name, folder, qualifier);
        } else {
            LOGGER.info("Skipping composition as this pipeline invocation will only publish events");
        }
    }

    private void compose(final String name, final Folder folder, final String qualifier) {
        LOGGER.info("Composing pipeline run results for {} in bucket gs://{}/{} with qualifier {}",
                name,
                reportBucket.getName(),
                name,
                qualifier);
        reportBucket.create(path(name, folder, "pipeline.version"), version.getBytes());
        try {
            reportBucket.create(path(name, folder, "run.log"), new FileInputStream("run.log"));
        } catch (FileNotFoundException e) {
            LOGGER.warn("No run.log found in working directory. Pipeline logs will not be available in results");
        }
        components.forEach(component -> {
            try {
                component.addToReport(storage, reportBucket, name);
            } catch (Exception e) {
                throw new RuntimeException(format("Unable add component [%s] to the final patient report.",
                        component.getClass().getSimpleName()), e);
            }
        });
        LOGGER.info("Composition complete for {} in bucket gs://{}/{} with qualifier {}", name, reportBucket.getName(), name, qualifier);
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