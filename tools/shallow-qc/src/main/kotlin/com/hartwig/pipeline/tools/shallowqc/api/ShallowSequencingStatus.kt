package com.hartwig.pipeline.tools.shallowqc.api

import io.github.oshai.kotlinlogging.KotlinLogging
import java.math.BigDecimal

private val logger = KotlinLogging.logger {}

internal fun computeShallowSequencingStatus(
    amberStatus: String,
    purpleQcStatus: String,
    purity: BigDecimal,
    tumCoverage: Double,
    refCoverage: Double
): String {
    val amberNoFail = !amberStatus.contains("FAIL")
    val purpleNoFail = !purpleQcStatus.contains("FAIL")
    val purityPasses = purity >= BigDecimal("0.08")
    val tumCovPasses = tumCoverage >= 8.0
    val refCovPasses = refCoverage >= 8.0
    val statuses = purpleQcStatus.split(",")
    val failsCount = statuses.count { it != "FAIL_NO_TUMOR" && it.startsWith("FAIL_") }
    val failTumorCount = statuses.count { it == "FAIL_NO_TUMOR" }

    logger.info {
        "Computing shallow sequencing status: amberStatus=[$amberStatus], purpleQcStatus=[$purpleQcStatus], " +
        "purity=[$purity], tumorCoverage=[$tumCoverage], referenceCoverage=[$refCoverage]"
    }

    val result = when {
        amberNoFail && purpleNoFail && purityPasses -> "PASS"
        amberNoFail && failsCount == 0 && (!purityPasses || failTumorCount > 0) && tumCovPasses && refCovPasses -> "FAIL"
        amberNoFail && failsCount == 0 && !purityPasses -> "ADD_SEQ"
        else -> "OTHER"
    }

    logger.info { "Computed shallow sequencing status: [$result]" }
    return result
}