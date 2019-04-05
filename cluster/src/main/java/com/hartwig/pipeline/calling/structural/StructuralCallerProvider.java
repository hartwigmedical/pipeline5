package com.hartwig.pipeline.calling.structural;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.germline.GermlineCaller;

public class StructuralCallerProvider {

    public static StructuralCallerProvider from(Arguments arguments){
        return new StructuralCallerProvider();
    }

    public StructuralCaller get() {
        //TODO: make a structural caller
        return null;
    }
}
