package com.hartwig.bcl2fastq.metadata;

import com.hartwig.bcl2fastq.ConvertedFastq;
import com.hartwig.bcl2fastq.Sample;

public interface ConversionMetadataApi {

    Sample find(String name);

    void link(Sample sample, ConvertedFastq fastq);
}
