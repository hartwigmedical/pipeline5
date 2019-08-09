package com.hartwig.pipeline.calling.structural.gridss.command;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;

public class RscriptFilter implements BashCommand {
    private final static String SCRIPT_DIR = VmDirectories.TOOLS + "/gridss-scripts/4.8.1";
    private final static String PON_DIR = VmDirectories.RESOURCES;

    private final String inputFile;
    private final String outputFile;
    private final String outputFullCompressedVcf;

    public RscriptFilter(final String inputFile, final String outputFile, final String outputFullCompressedVcf) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.outputFullCompressedVcf = outputFullCompressedVcf;
    }

    @Override
    public String asBash() {
        return String.format("Rscript %s/gridss_somatic_filter.R -p %s -i %s -o %s -f %s -s %s",
                SCRIPT_DIR, PON_DIR, inputFile, outputFile, outputFullCompressedVcf, SCRIPT_DIR);
    }
}
