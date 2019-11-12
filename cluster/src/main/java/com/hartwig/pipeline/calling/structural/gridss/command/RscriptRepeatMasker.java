package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

public class RscriptRepeatMasker extends GridssRscript {
    private final String inputPath;
    private final String outputPath;
    private final String repeatMaskerDb;

    public RscriptRepeatMasker(String inputPath, String outputPath, String repeatMaskerDb) {

        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.repeatMaskerDb = repeatMaskerDb;
    }

    @Override
    String scriptName() {
        return "gridss_annotate_insertions_repeatmaster.R";
    }

    @Override
    String arguments() {
        return format("--input %s --output %s --repeatmasker %s --scriptdir %s", inputPath, outputPath, repeatMaskerDb, GRIDSS_RSCRIPT_DIR);
    }
}
