package com.hartwig.batch.input;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize
public class InputBundle {
    @JsonProperty
    private List<InputFileDescriptor> inputs;

    public InputBundle(Collection<InputFileDescriptor> inputs) {
        if (inputs == null || inputs.isEmpty()) {
            throw new IllegalArgumentException("Null or empty inputs!");
        }
        this.inputs = new ArrayList<>(inputs);
    }

    public InputFileDescriptor get() {
        return inputs.get(0);
    }

    public InputFileDescriptor get(String key) {
        return inputs.stream().filter(i -> i.name().equals(key)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException(format("No key [%s] in input", key)));
    }
}
