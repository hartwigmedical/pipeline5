package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.tools.Versions;

public class RscriptFilter implements BashCommand {
    private final static String SCRIPT_DIR = format("%s/gridss/%s", VmDirectories.TOOLS, Versions.GRIDSS);
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
        return format("Rscript %s/gridss_somatic_filter.R -p %s -i %s -o %s -f %s -s %s",
                SCRIPT_DIR, PON_DIR, inputFile, outputFile, outputFullCompressedVcf, SCRIPT_DIR);
    }
}
