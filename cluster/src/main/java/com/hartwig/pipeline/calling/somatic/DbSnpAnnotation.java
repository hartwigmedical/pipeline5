package com.hartwig.pipeline.calling.somatic;

import com.google.common.collect.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.command.BcfToolsAnnotationCommand;
import com.hartwig.pipeline.calling.command.TabixCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;

class DbSnpAnnotation extends SubStage {

    private final String dbsnp;

    DbSnpAnnotation(final String dbsnp) {
        super("dbsnp.annotated", OutputFile.GZIPPED_VCF);
        this.dbsnp = dbsnp;
    }

    @Override
    public BashStartupScript bash(final OutputFile input, final OutputFile output, final BashStartupScript bash) {
        return bash.addCommand(new BcfToolsAnnotationCommand(Lists.newArrayList(dbsnp, "-c", "ID"), input.path(), output.path()))
                .addCommand(new TabixCommand(output.path()));
    }
}
