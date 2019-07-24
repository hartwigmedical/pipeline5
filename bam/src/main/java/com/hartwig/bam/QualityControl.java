package com.hartwig.bam;

import com.hartwig.io.InputOutput;

public interface QualityControl<I> {

    QCResult check(InputOutput<I> toQC);
}
