package com.hartwig.pipeline.metadata;

import com.hartwig.api.model.Sample;
import com.hartwig.pipeline.Arguments;

public class Anonymizer {

    private final Arguments arguments;

    public Anonymizer(final Arguments arguments) {
        this.arguments = arguments;
    }

    public String sampleName(final Sample sample) {
        return arguments.anonymize() ? sample.getBarcode() : sample.getName();
    }
}
