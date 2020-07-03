package com.hartwig.pipeline.calling.structural.gridss.stage;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateInsertedSequence;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;

public class GridssAnnotation extends SubStage {

    private final String virusReferenceGenomePath;
    private final ResourceFiles resourceFiles;

    public GridssAnnotation(final ResourceFiles resourceFiles, final String virusReferenceGenomePath) {
        super("gridss.unfiltered", OutputFile.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
        this.virusReferenceGenomePath = virusReferenceGenomePath;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        List<BashCommand> result = Lists.newArrayList();

        // Repeat Masker
        final String interimFile = input.path() + ".repeatmasker.vcf.gz";
        result.add(AnnotateInsertedSequence.repeatMasker(input.path(),
                interimFile,
                resourceFiles.refGenomeFile(),
                resourceFiles.gridssRepeatMaskerDbBed()));

        // Viral Annotation
        result.add(AnnotateInsertedSequence.viralAnnotation(interimFile, output.path(), virusReferenceGenomePath));

        return result;
    }
}
