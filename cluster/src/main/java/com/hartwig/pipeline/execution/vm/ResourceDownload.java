package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.io.RuntimeBucket;
import com.hartwig.pipeline.resource.ResourceLocation;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Stream.of;

public class ResourceDownload implements BashCommand {

    private static final String RESOURCES_PATH = "/data/resources";
    private final ResourceLocation resource;
    private final RuntimeBucket runtimeBucket;

    public ResourceDownload(final ResourceLocation resourceLocation, final RuntimeBucket runtimeBucket) {
        this.resource = resourceLocation;
        this.runtimeBucket = runtimeBucket;
    }

    @Override
    public String asBash() {
        return String.format("gsutil -m cp gs://%s/%s/* %s", runtimeBucket.name(), resource.bucket(), RESOURCES_PATH);
    }

    List<String> getLocalPaths() {
        return resource.files().stream().map(this::fileName).map(file -> RESOURCES_PATH + "/" + file).collect(Collectors.toList());
    }

    public String find(String... extensions) {
        return getLocalPaths().stream()
                .filter(file -> of(extensions).anyMatch(file::endsWith))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format(
                        "No file with extension(s) %s was found in resource location [%s]",
                        Arrays.toString(extensions),
                        this)));
    }

    private String fileName(final String path) {
        String[] pathSplit = path.split("/");
        return pathSplit[pathSplit.length - 1];
    }
}
