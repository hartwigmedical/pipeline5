package com.hartwig.pipeline.calling.germline;

import com.hartwig.pipeline.Arguments;

public class GermlineCallerProvider {

    public static GermlineCallerProvider from(Arguments arguments){
        return new GermlineCallerProvider();
    }

    public GermlineCaller get() {
        //TODO: make a germline caller
        return null;
    }
}
