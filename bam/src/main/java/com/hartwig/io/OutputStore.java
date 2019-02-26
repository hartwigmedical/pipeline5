package com.hartwig.io;

import com.hartwig.patient.Sample;

public interface OutputStore<P> {

    void store(InputOutput<P> inputOutput);

    boolean exists(final Sample sample);

    void clear();
}
