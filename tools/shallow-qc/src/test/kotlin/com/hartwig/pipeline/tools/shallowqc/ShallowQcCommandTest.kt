package com.hartwig.pipeline.tools.shallowqc

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.hartwig.pipeline.tools.shallowqc.api.ShallowQcCommand
import com.hartwig.pipeline.tools.shallowqc.api.ShallowQcResult
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import picocli.CommandLine
import java.io.File

class ShallowQcCommandTest {

    @TempDir
    lateinit var outputDir: File

    private val mapper = jacksonObjectMapper()

    private fun runScenario(scenario: String, sampleId: String = "TUMOR-01"): ShallowQcResult {
        val inputDir = File(javaClass.getResource("/pipeline-output/$scenario")!!.toURI())
        CommandLine(ShallowQcCommand()).execute(sampleId, inputDir.absolutePath, "--output-dir", outputDir.absolutePath)
        return mapper.readValue(outputDir.resolve("$sampleId.shallow-qc.json"))
    }

    @Test
    fun `PASS when amber and purple pass and purity is sufficient`() {
        val result = runScenario("pass")

        assertEquals("PASS", result.shallowSequencingStatus)
        assertEquals("TU000001", result.tumorIsolationBarcode)
        assertEquals("TR000001", result.referenceIsolationBarcode)
        assertEquals(listOf("PASS"), result.purpleStatus)
        assertEquals("NORMAL", result.purpleFitMethod)
        assertEquals("PASS", result.amberStatus)
        assertEquals(90.0, result.tumorCellPurity)
    }

    @Test
    fun `FAIL when purity is below threshold but coverage is sufficient`() {
        val result = runScenario("fail")

        assertEquals("FAIL", result.shallowSequencingStatus)
        assertEquals("PASS", result.amberStatus)
        assertEquals(5.0, result.tumorCellPurity)
    }

    @Test
    fun `ADD_SEQ when purity is below threshold and tumor coverage is insufficient`() {
        val result = runScenario("add-seq")

        assertEquals("ADD_SEQ", result.shallowSequencingStatus)
        assertEquals("PASS", result.amberStatus)
        assertEquals(5.0, result.tumorCellPurity)
    }

    @Test
    fun `OTHER when amber failed`() {
        val result = runScenario("other")

        assertEquals("OTHER", result.shallowSequencingStatus)
        assertEquals("FAIL", result.amberStatus)
    }
}