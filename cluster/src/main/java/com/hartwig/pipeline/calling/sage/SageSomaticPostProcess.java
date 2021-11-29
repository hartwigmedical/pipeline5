package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.stages.SubStageInputOutput;

public class SageSomaticPostProcess extends SubStage {

    public static final String SAGE_SOMATIC_FILTERED = "sage.somatic.filtered";
    private final ResourceFiles resourceFiles;
    private final SubStageInputOutput tumorSampleName;

    public SageSomaticPostProcess(final String tumorSampleName, final ResourceFiles resourceFiles) {
        super(SAGE_SOMATIC_FILTERED, FileTypes.GZIPPED_VCF);
        this.tumorSampleName = SubStageInputOutput.empty(tumorSampleName);
        this.resourceFiles = resourceFiles;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        final List<BashCommand> result = Lists.newArrayList();

        SubStage passFilter = new PassFilter();
        SubStage mappabilityAnnotation =
                new MappabilityAnnotation(resourceFiles.out150Mappability(), resourceFiles.mappabilityHDR());
        SubStage ponAnnotation = new PonAnnotation("sage.pon", resourceFiles.sageGermlinePon(), "PON_COUNT", "PON_MAX");
        SubStage ponFilter = new PonFilter(resourceFiles.version());

        OutputFile passFilterFile = passFilter.apply(tumorSampleName).outputFile();
        OutputFile mappabilityAnnotationFile = mappabilityAnnotation.apply(tumorSampleName).outputFile();
        OutputFile ponAnnotationFile = ponAnnotation.apply(tumorSampleName).outputFile();
        OutputFile ponFilterFile = ponFilter.apply(tumorSampleName).outputFile();

        result.addAll(passFilter.bash(input, passFilterFile));
        result.addAll(mappabilityAnnotation.bash(passFilterFile, mappabilityAnnotationFile));
        result.addAll(ponAnnotation.bash(mappabilityAnnotationFile, ponAnnotationFile));
        result.addAll(ponFilter.bash(ponAnnotationFile, ponFilterFile));
        return result;
    }
}
