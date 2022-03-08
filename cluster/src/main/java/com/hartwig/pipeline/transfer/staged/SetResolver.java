package com.hartwig.pipeline.transfer.staged;

import com.hartwig.api.SetApi;
import com.hartwig.api.helpers.OnlyOne;
import com.hartwig.api.model.SampleSet;
import com.hartwig.pipeline.Arguments;

public interface SetResolver {
    SampleSet resolve(String name, boolean useOnlyDbSets);

    static SetResolver forApi(SetApi setApi) {
        return (name, useOnlyDbSets) -> OnlyOne.of(setApi.list(name, null, useOnlyDbSets ? true : null), SampleSet.class);
    }

    static SetResolver forLocal() {
        return (name, useOnlyDbSets) -> new SampleSet().id(0L);
    }
}
