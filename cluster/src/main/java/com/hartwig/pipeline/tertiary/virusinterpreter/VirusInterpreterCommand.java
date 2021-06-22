package com.hartwig.pipeline.tertiary.virusinterpreter;

import java.util.List;

import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.tools.Versions;

public class VirusInterpreterCommand extends JavaJarCommand {

    public VirusInterpreterCommand(final String sampleId, final String virusBreakendSummary, final String taxonomyDbTsv,
            final String virusInterpretationTsv, final String virusBlacklistTsv, final String outputDir) {
        super("virus-interpreter",
                Versions.VIRUS_INTERPRETER,
                "virus-interpreter.jar",
                "2G",
                List.of("-sample_id",
                        sampleId,
                        "-virus_breakend_tsv",
                        virusBreakendSummary,
                        "-taxonomy_db_tsv",
                        taxonomyDbTsv,
                        "-virus_interpretation_tsv",
                        virusInterpretationTsv,
                        "-virus_blacklist_tsv",
                        virusBlacklistTsv,
                        "-output_dir",
                        outputDir));
    }
}
