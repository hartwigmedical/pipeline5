package com.hartwig.pipeline.tools.shallowqc.model

data class PipelineMetadata(
    val reference: SampleMetadata,
    val tumor: SampleMetadata
)