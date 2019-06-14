package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.io.sbp.SBPRestApi;

public class SetMetadataApiProvider {

    private final Arguments arguments;

    private SetMetadataApiProvider(final Arguments arguments) {
        this.arguments = arguments;
    }

    public static SetMetadataApiProvider from(final Arguments arguments) {
        return new SetMetadataApiProvider(arguments);
    }

    public SetMetadataApi get() {
        return arguments.sbpApiRunId().<SetMetadataApi>map(setId -> new SbpSetMetadataApi(setId, SBPRestApi.newInstance(arguments))).orElse(
                new LocalSetMetadataApi(arguments.setId()));
    }
}