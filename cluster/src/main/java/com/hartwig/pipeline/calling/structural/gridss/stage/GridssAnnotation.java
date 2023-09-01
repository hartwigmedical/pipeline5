package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.storage.OutputFile;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateInsertedSequence;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

import java.util.List;

public class GridssAnnotation extends SubStage {

    public static final String GRIDSS_ANNOTATED = "gridss.unfiltered";
    private final String virusReferenceGenomePath;
    private final ResourceFiles resourceFiles;

    public GridssAnnotation(final ResourceFiles resourceFiles) {
        super(GRIDSS_ANNOTATED, FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
        this.virusReferenceGenomePath = resourceFiles.gridssVirusRefGenomeFile();
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        List<BashCommand> result = Lists.newArrayList();

        // Viral Annotation
        result.add(AnnotateInsertedSequence.viralAnnotation(input.path(), output.path(), virusReferenceGenomePath));
        return result;
    }
}
