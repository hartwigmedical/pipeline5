package com.hartwig.pipeline.calling.sage;

import java.util.function.Function;

import com.hartwig.pipeline.input.SomaticRunMetadata;

public interface OutputTemplate extends Function<SomaticRunMetadata, String> {
}
