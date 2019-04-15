package com.hartwig.pipeline.execution.vm;

import com.hartwig.pipeline.resource.Resource;

public class ResourceDownload implements BashCommand {

    private final Resource resource;

    public ResourceDownload(final Resource referenceGenomeResource) {
        this.resource = referenceGenomeResource;
    }

    @Override
    public String asBash() {
        return String.format("gsutil -m cp gs://%s/* /data/resources/", resource.getTargetBucket());
    }
}
