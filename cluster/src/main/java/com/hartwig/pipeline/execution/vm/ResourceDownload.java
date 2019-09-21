package com.hartwig.pipeline.execution.vm;

import static java.util.stream.Stream.of;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class ResourceDownload implements BashCommand {

    private static final String RESOURCES_PATH = "/data/resources";
    private final Resource resource;
    private final ResourceLocation resourceLocation;

    public ResourceDownload(final Resource resource, final ResourceLocation resourceLocation) {
        this.resource = resource;
        this.resourceLocation = resourceLocation;
    }

    @Override
    public String asBash() {
        return String.format("gsutil -qm cp gs://%s/* %s", resourceLocation.bucket(), RESOURCES_PATH);
    }

    public Resource getResource() {
        return resource;
    }

    List<String> getLocalPaths() {
        return resourceLocation.files().stream().map(this::fileName).map(file -> RESOURCES_PATH + "/" + file).collect(Collectors.toList());
    }

    public String find(String... extensions) {
        return getLocalPaths().stream()
                .filter(file -> of(extensions).anyMatch(file::endsWith))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "No file with extension(s) %s was found in resource location [%s]",
                        Arrays.toString(extensions),
                        resourceLocation)));
    }

    private String fileName(final String path) {
        String[] pathSplit = path.split("/");
        return pathSplit[pathSplit.length - 1];
    }

    public static ResourceDownload from(final Storage storage, final String resourceBucket, final String resource,
            final RuntimeBucket runtimeBucket) {
        return from(runtimeBucket, new Resource(storage, resourceBucket, resource));
    }

    public static ResourceDownload from(final RuntimeBucket runtimeBucket, final Resource resource) {
        return new ResourceDownload(resource, resource.copyInto(runtimeBucket));
    }
}
