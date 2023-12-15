package com.hartwig.pipeline.calling.germline;

import java.util.Collections;
import java.util.List;

import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.calling.germline.command.GatkHaplotypeCallerCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

public class GatkGermlineCaller extends SubStage {

    private final String inputBam;
    private final String referenceFasta;

    GatkGermlineCaller(final String inputBam, final String referenceFasta) {
        super("raw_germline_caller", FileTypes.GZIPPED_VCF);
        this.inputBam = inputBam;
        this.referenceFasta = referenceFasta;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return Collections.singletonList(new GatkHaplotypeCallerCommand(inputBam, referenceFasta, output.path()));
    }
}