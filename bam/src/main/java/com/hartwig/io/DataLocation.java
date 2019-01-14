package com.hartwig.io;

import com.hartwig.patient.Sample;

public interface DataLocation {

    String uri(OutputType output, Sample sample);

    String rootUri();
}
