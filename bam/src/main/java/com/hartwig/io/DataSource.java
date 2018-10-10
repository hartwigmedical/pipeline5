package com.hartwig.io;

import com.hartwig.patient.Sample;

public interface DataSource<P> {
    InputOutput<P> extract(Sample sample);
}
