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
    private final boolean applyRepeatMasker;

    public GridssAnnotation(final ResourceFiles resourceFiles, final String virusReferenceGenomePath, final boolean applyRepeatMasker) {
        super("gridss.unfiltered", OutputFile.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
        this.virusReferenceGenomePath = virusReferenceGenomePath;
        this.applyRepeatMasker = applyRepeatMasker;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        List<BashCommand> result = Lists.newArrayList();

        // Repeat Masker
        String interimFile = input.path();
        if (applyRepeatMasker) {
            interimFile += ".repeatmasker.vcf.gz";
            result.add(AnnotateInsertedSequence.repeatMasker(input.path(), interimFile, resourceFiles.refGenomeFile(), resourceFiles.gridssRepeatMaskerDbBed()));
        }

        // Viral Annotation
        result.add(AnnotateInsertedSequence.viralAnnotation(interimFile, output.path(), virusReferenceGenomePath));
        return result;
    }
}
