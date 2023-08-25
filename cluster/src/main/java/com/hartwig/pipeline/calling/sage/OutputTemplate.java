package com.hartwig.pipeline.calling.sage;

import com.hartwig.computeengine.input.SomaticRunMetadata;

import java.util.function.Function;

public interface OutputTemplate extends Function<SomaticRunMetadata, String> {
}
