package com.hartwig.io;

import com.hartwig.patient.Sample;

public interface OutputStore<P> {

    void store(InputOutput<P> inputOutput);

    void store(InputOutput<P> inputOutput, String suffix);

    boolean exists(final Sample sample);

    void clear();
}
