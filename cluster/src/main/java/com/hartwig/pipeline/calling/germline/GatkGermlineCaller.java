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
    private final String dbsnp;

    GatkGermlineCaller(final String inputBam, final String referenceFasta, final String dbsnp) {
        super("germline_calling", "vcf.gz");
        this.inputBam = inputBam;
        this.referenceFasta = referenceFasta;
        this.dbsnp = dbsnp;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, OutputFile output) {
        return Collections.singletonList(new GatkHaplotypeCallerCommand(inputBam, referenceFasta, dbsnp, output.path()));
    }
}