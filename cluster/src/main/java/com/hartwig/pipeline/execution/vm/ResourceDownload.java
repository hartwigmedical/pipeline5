package com.hartwig.pipeline.execution.vm;

import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.ResourceLocation;

public class ResourceDownload implements BashCommand {

    private static final String RESOURCES_PATH = "/data/resources";
    private final ResourceLocation resource;
    private final RuntimeBucket runtimeBucket;

    public ResourceDownload(final ResourceLocation referenceGenomeResource, final RuntimeBucket runtimeBucket) {
        this.resource = referenceGenomeResource;
        this.runtimeBucket = runtimeBucket;
    }

    @Override
    public String asBash() {
        return String.format("gsutil -m cp gs://%s/%s/* %s", runtimeBucket.name(), resource.bucket(), RESOURCES_PATH);
    }

    public List<String> getLocalPaths() {
        return resource.files().stream().map(this::fileName).map(file -> RESOURCES_PATH + "/" + file).collect(Collectors.toList());
    }

    private String fileName(final String path) {
        String[] pathSplit = path.split("/");
        return pathSplit[pathSplit.length - 1];
    }
}
