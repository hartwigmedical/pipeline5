package com.hartwig.pipeline;

import com.hartwig.io.InputOutput;

public interface QualityControl<I> {

    QCResult check(InputOutput<I> toQC);
}
