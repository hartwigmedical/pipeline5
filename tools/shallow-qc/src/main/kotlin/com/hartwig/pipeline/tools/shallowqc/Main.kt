package com.hartwig.pipeline.tools.shallowqc

import com.hartwig.pipeline.tools.shallowqc.api.ShallowQcCommand
import picocli.CommandLine
import kotlin.system.exitProcess

fun main(args: Array<String>) {
    exitProcess(CommandLine(ShallowQcCommand()).execute(*args))
}