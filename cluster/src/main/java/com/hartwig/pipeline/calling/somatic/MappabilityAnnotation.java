package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.stages.SubStage;

class MappabilityAnnotation extends SubStage {

    private final String bed;
    private final String hdr;

    MappabilityAnnotation(final String bed, final String hdr) {
        super("mappability.annotated", FileTypes.GZIPPED_VCF);
        this.bed = bed;
        this.hdr = hdr;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex()
                .addAnnotationWithHeader(bed, "CHROM,FROM,TO,-,MAPPABILITY", hdr)
                .build();
    }

}
