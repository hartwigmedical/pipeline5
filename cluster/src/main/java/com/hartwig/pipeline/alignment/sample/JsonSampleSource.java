package com.hartwig.pipeline.alignment.sample;

import java.io.FileInputStream;
import java.io.IOException;

import com.hartwig.patient.ReferenceTumorPair;
import com.hartwig.patient.Sample;
import com.hartwig.pipeline.jackson.ObjectMappers;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;

public class JsonSampleSource implements SampleSource {

    private final ReferenceTumorPair pair;

    public JsonSampleSource(final String filename) {
        try {
            this.pair = ObjectMappers.get().readValue(new String(new FileInputStream(filename).readAllBytes()), ReferenceTumorPair.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Sample sample(final SingleSampleRunMetadata metadata) {
        try {
            if (metadata.type().equals(SingleSampleRunMetadata.SampleType.REFERENCE)) {
                return pair.reference();
            } else {
                return pair.tumor();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
