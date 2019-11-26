package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.resource.ResourceNames;

public class RscriptFilter extends GridssRscript {
    private final String inputFile;
    private final String outputFile;
    private final String outputFullCompressedVcf;

    public RscriptFilter(final String inputFile, final String outputFile, final String outputFullCompressedVcf) {
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.outputFullCompressedVcf = outputFullCompressedVcf;
    }

    @Override
    String scriptName() {
        return "gridss_somatic_filter.R";
    }

    @Override
    String arguments() {
        return format("-p %s -i %s -o %s -f %s -s %s",
                VmDirectories.RESOURCES + "/" + ResourceNames.GRIDSS_PON,
                inputFile,
                outputFile,
                outputFullCompressedVcf,
                GRIDSS_RSCRIPT_DIR);
    }
}
