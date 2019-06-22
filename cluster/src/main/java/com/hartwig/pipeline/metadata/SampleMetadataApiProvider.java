package com.hartwig.pipeline.metadata;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.io.sbp.SBPRestApi;

public class SampleMetadataApiProvider {

    private final Arguments arguments;

    private SampleMetadataApiProvider(final Arguments arguments) {
        this.arguments = arguments;
    }

    public SampleMetadataApi get() {
        return arguments.sbpApiSampleId().<SampleMetadataApi>map(sbpSampleId -> new SbpSampleMetadataApi(SBPRestApi.newInstance(arguments),
                sbpSampleId)).orElse(new LocalSampleMetadataApi(arguments.sampleId()));
    }

    public static SampleMetadataApiProvider from(final Arguments arguments) {
        return new SampleMetadataApiProvider(arguments);
    }
}