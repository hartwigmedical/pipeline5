package com.hartwig.pipeline.tools.shallowqc.api

data class ShallowQcResult(
    val tumorIsolationBarcode: String,
    val referenceIsolationBarcode: String,
    val purpleStatus: List<String>,
    val purpleFitMethod: String,
    val amberStatus: String,
    val tumorCellPurity: Double,
    val shallowSequencingStatus: String
)