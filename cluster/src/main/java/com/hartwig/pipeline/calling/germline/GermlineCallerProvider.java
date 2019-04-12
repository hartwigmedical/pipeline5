package com.hartwig.pipeline.calling.germline;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.resource.Resources;

public class GermlineCallerProvider {

    @SuppressWarnings("FieldCanBeLocal")
    private final Arguments arguments;
    private final Storage storage;

    private GermlineCallerProvider(final Arguments arguments, final Storage storage) {
        this.arguments = arguments;
        this.storage = storage;
    }

    public static GermlineCallerProvider from(GoogleCredentials credentials, Storage storage, Arguments arguments){
        return new GermlineCallerProvider(arguments, storage);
    }

    public GermlineCaller get() {
        return new GermlineCaller(arguments);
    }
}
