package com.hartwig.pipeline.metadata;

import com.google.cloud.storage.Storage;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.alignment.sample.JsonSampleSource;
import com.hartwig.pipeline.sbpapi.SbpRestApi;
import com.hartwig.pipeline.transfer.google.GoogleArchiver;

public class SomaticMetadataApiProvider {

    private final Arguments arguments;
    private final Storage storage;

    private SomaticMetadataApiProvider(final Arguments arguments, final Storage storage) {
        this.arguments = arguments;
        this.storage = storage;
    }

    public static SomaticMetadataApiProvider from(final Arguments arguments, final Storage storage) {
        return new SomaticMetadataApiProvider(arguments, storage);
    }

    public SomaticMetadataApi get() {
        return arguments.sbpApiRunId().<SomaticMetadataApi>map(setId -> new SbpSomaticMetadataApi(arguments,
                setId,
                SbpRestApi.newInstance(arguments.sbpApiUrl()),
                storage.get(arguments.outputBucket()),
                new GoogleArchiver(arguments))).orElseGet(() -> new LocalSomaticMetadata(arguments,
                arguments.sampleJson()
                        .map(JsonSampleSource::new)
                        .orElseThrow(() -> new IllegalArgumentException("Sample JSON must be provided when running in local mode"))));
    }
}
