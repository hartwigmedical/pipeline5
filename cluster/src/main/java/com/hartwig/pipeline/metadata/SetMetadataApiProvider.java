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

    public SomaticMetadataApi get() {
        return arguments.sbpApiRunId().<SomaticMetadataApi>map(setId -> new SbpSomaticMetadataApi(arguments, setId, SBPRestApi.newInstance(arguments))).orElse(
                new LocalSetMetadataApi(arguments));
    }
}