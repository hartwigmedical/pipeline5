package com.hartwig.pipeline.metadata;

import java.time.LocalDateTime;
import java.time.ZoneId;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.io.sbp.ResultsPublisherProvider;
import com.hartwig.pipeline.io.sbp.SbpRestApi;

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
        return arguments.sbpApiRunId().<SomaticMetadataApi>map(setId -> new SbpSomaticMetadataApi(arguments,
                setId,
                SbpRestApi.newInstance(arguments),
                ResultsPublisherProvider.from(arguments, storage).get(),
                LocalDateTime.now(ZoneId.of("UTC")))).orElse(new LocalSetMetadataApi(arguments));
    }
}
