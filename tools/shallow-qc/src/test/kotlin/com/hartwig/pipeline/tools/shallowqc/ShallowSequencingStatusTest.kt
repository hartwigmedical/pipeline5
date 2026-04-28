package com.hartwig.pipeline.tools.shallowqc

import com.hartwig.pipeline.tools.shallowqc.api.computeShallowSequencingStatus
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class ShallowSequencingStatusTest {

    @Test
    fun `returns PASS when amber and purple have no failures and purity is sufficient`() {
        assertEquals(
            "PASS",
            computeShallowSequencingStatus("PASS", "WARN_DELETED_GENES,WARN_GENDER_MISMATCH", BigDecimal("1.0"), 10.0, 10.0)
        )
    }

    @Test
    fun `returns FAIL when purity is below threshold but coverage is sufficient`() {
        assertEquals(
            "FAIL",
            computeShallowSequencingStatus("PASS", "PASS", BigDecimal("0.05"), 10.0, 10.0)
        )
    }

    @Test
    fun `returns FAIL when purple status is FAIL_NO_TUMOR and coverage is sufficient`() {
        assertEquals(
            "FAIL",
            computeShallowSequencingStatus("PASS", "FAIL_NO_TUMOR", BigDecimal("0.05"), 10.0, 10.0)
        )
    }

    @Test
    fun `returns ADD_SEQ when purity is below threshold and tumor coverage is insufficient`() {
        assertEquals(
            "ADD_SEQ",
            computeShallowSequencingStatus("PASS", "PASS", BigDecimal("0.05"), 5.0, 10.0)
        )
    }

    @Test
    fun `returns ADD_SEQ when purity is below threshold and reference coverage is insufficient`() {
        assertEquals(
            "ADD_SEQ",
            computeShallowSequencingStatus("PASS", "PASS", BigDecimal("0.05"), 10.0, 5.0)
        )
    }

    @Test
    fun `returns OTHER when amber has a failure`() {
        assertEquals(
            "OTHER",
            computeShallowSequencingStatus("FAIL", "PASS", BigDecimal("1.0"), 10.0, 10.0)
        )
    }

    @Test
    fun `returns OTHER when purple has non-tumor failures`() {
        assertEquals(
            "OTHER",
            computeShallowSequencingStatus("PASS", "FAIL_CONTAMINATION", BigDecimal("0.05"), 10.0, 10.0)
        )
    }
}