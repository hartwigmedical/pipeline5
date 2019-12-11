package com.hartwig.batch.input;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.String.format;

public class InputBundle {
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
