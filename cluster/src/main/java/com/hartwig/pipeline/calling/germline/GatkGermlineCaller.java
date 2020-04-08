package com.hartwig.pipeline.calling.germline;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.germline.command.GatkHaplotypeCallerCommand;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

public class GatkGermlineCaller extends SubStage {

    private final String inputBam;
    private final String referenceFasta;

    GatkGermlineCaller(final String inputBam, final String referenceFasta) {
        super("raw_germline_caller", "vcf.gz");
        this.inputBam = inputBam;
        this.referenceFasta = referenceFasta;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, OutputFile output) {
        return Collections.singletonList(new GatkHaplotypeCallerCommand(inputBam, referenceFasta, output.path()));
    }
}