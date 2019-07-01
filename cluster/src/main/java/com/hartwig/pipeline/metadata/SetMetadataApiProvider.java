package com.hartwig.pipeline.metadata;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.io.sbp.ResultsPublisherProvider;
import com.hartwig.pipeline.io.sbp.SBPRestApi;

public class SetMetadataApiProvider {

    private final Arguments arguments;
    private final Storage storage;

    private SetMetadataApiProvider(final Arguments arguments, final Storage storage) {
        this.arguments = arguments;
        this.storage = storage;
    }

    public static SetMetadataApiProvider from(final Arguments arguments, final Storage storage) {
        return new SetMetadataApiProvider(arguments, storage);
    }

    public SomaticMetadataApi get() {
        return arguments.sbpApiRunId()
                .<SomaticMetadataApi>map(setId -> new SbpSomaticMetadataApi(arguments, setId, SBPRestApi.newInstance(arguments),
                        ResultsPublisherProvider.from(arguments, storage).get()))
                .orElse(new LocalSetMetadataApi(arguments));
    }
}