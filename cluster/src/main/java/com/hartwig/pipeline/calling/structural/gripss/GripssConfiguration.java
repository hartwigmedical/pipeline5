package com.hartwig.pipeline.calling.structural.gripss;

import com.hartwig.pipeline.calling.sage.OutputTemplate;

import org.immutables.value.Value;

@Value.Immutable
public interface GripssConfiguration {

    static final String GRIPSS_SOMATIC_FILTERED = ".gripss.filtered.somatic.";
    static final String GRIPSS_SOMATIC_UNFILTERED = ".gripss.somatic.";
    static final String GRIPSS_GERMLINE_FILTERED = ".gripss.filtered.germline.";
    static final String GRIPSS_GERMLINE_UNFILTERED = ".gripss.germline.";
    String GERMLINE_NAMESPACE = "gripss_germline";

    OutputTemplate filteredVcf();

    OutputTemplate unfilteredVcf();

    static GripssConfiguration somatic() {
        return null;
    }

    static GripssConfiguration germline() {
        return null;
    }

}