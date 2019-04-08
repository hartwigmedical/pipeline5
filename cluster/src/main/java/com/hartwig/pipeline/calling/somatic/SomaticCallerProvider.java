package com.hartwig.pipeline.calling.somatic;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.germline.GermlineCaller;

public class SomaticCallerProvider {

    public static SomaticCallerProvider from(Arguments arguments){
        return new SomaticCallerProvider();
    }

    public SomaticCaller get() {
        //TODO: make a somatic caller
        return new SomaticCaller();
    }
}
