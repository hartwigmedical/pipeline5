package com.hartwig.pipeline.calling.sage;

import java.util.function.Function;

import com.hartwig.pipeline.metadata.SomaticRunMetadata;

public interface OutputTemplate extends Function<SomaticRunMetadata, String> {
}
