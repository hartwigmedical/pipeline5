package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.Arguments;

public class GermlineCallerProvider {

    @SuppressWarnings({ "FieldCanBeLocal", "unused" })
    private final Arguments arguments;

    private GermlineCallerProvider(final Arguments arguments) {
        this.arguments = arguments;
    }

    public static GermlineCallerProvider from(Arguments arguments){
        return new GermlineCallerProvider(arguments);
    }

    public GermlineCaller get() {
        return new GermlineCaller();
    }
}
