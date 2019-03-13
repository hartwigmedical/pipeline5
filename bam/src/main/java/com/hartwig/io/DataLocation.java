package com.hartwig.io;

import com.hartwig.patient.Sample;

public interface DataLocation {

    String uri(Sample sample, String extension);

    String rootUri();
}
