package com.hartwig.pipeline.report;

import static java.lang.String.format;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.RunTag;
import com.hartwig.pipeline.StageOutput;
import com.hartwig.pipeline.metadata.ObjectMappers;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipelineResults {

    private static final Logger LOGGER = LoggerFactory.getLogger(PipelineResults.class);

    private final String version;
    private final Storage storage;
    private final Bucket reportBucket;
    private final Arguments arguments;
    private final List<ReportComponent> components = new ArrayList<>();

    PipelineResults(final String version, final Storage storage, final Bucket reportBucket, final Arguments arguments) {
        this.version = version;
        this.storage = storage;
        this.reportBucket = reportBucket;
        this.arguments = arguments;
    }

    public <T extends StageOutput> T add(T stageOutput) {
        if (stageOutput != null) {
            components.addAll(stageOutput.reportComponents());
        }
        return stageOutput;
    }

    public void compose(SomaticRunMetadata metadata) {
        String name = metadata.runName();
        Folder folder = Folder.from(metadata);
        writeMetadata(metadata, name, folder);
        compose(name, folder);
    }

    public void compose(SingleSampleRunMetadata metadata) {
        String name = RunTag.apply(arguments, metadata.sampleId());
        Folder folder = Folder.from(metadata);
        writeMetadata(metadata, name, folder);
        compose(name, folder);
    }

    private void compose(String name, Folder folder) {
        LOGGER.info("Composing pipeline run results for {} in bucket gs://{}/{}", name, reportBucket.getName(), name);
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
                LOGGER.error(format("Unable add component [%s] to the final patient report.", component.getClass().getSimpleName()), e);
            }
        });
    }

    private void writeMetadata(final Object metadata, final String name, final Folder folder) {
        try {
            reportBucket.create(path(name, folder, "metadata.json"), ObjectMappers.get().writeValueAsBytes(metadata));
        } catch (JsonProcessingException e) {
            LOGGER.warn("Unable to write metadata file for metadata object [{}]", metadata);
        }
    }

    private String path(final String name, final Folder folder, final String fileName) {
        return String.format("%s/%s/%s", name, folder.name(), fileName);
    }

}