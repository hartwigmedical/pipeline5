package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsCommandBuilder;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;

class DbSnpAnnotation extends SubStage {

    private final String dbsnp;

    DbSnpAnnotation(final String dbsnp) {
        super("dbsnp.annotated", OutputFile.GZIPPED_VCF);
        this.dbsnp = dbsnp.endsWith("gz") ? dbsnp : dbsnp + ".gz";
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandBuilder(input.path(), output.path())
                .addAnnotation(dbsnp, "ID")
                .buildAndIndex();
    }
}
