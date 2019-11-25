package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsAnnotationCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
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
        return ImmutableList.of(new BcfToolsAnnotationCommand(Lists.newArrayList(dbsnp, "-c", "ID"), input.path(), output.path()),
                new TabixCommand(output.path()));
    }
}
