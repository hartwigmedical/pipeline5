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
    private final SubStageInputOutput tumorSampleName;

    public SageSomaticPostProcess(final String tumorSampleName) {
        super(SAGE_SOMATIC_FILTERED, FileTypes.GZIPPED_VCF);
        this.tumorSampleName = SubStageInputOutput.empty(tumorSampleName);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        final List<BashCommand> result = Lists.newArrayList();

        SubStage passFilter = new PassFilter();

        OutputFile finalOutputFile = OutputFile.of(tumorSampleName.sampleName(), SAGE_SOMATIC_FILTERED, FileTypes.GZIPPED_VCF);
        result.addAll(passFilter.bash(input, finalOutputFile));

        /*
        // OutputFile passFilterFile = passFilter.apply(tumorSampleName).outputFile();
        result.addAll(mappabilityAnnotation.bash(passFilterFile, finalOutputFile));
        result.addAll(mappabilityAnnotation.bash(passFilterFile, mappabilityAnnotationFile));
        SubStage ponAnnotation = new PonAnnotation("sage.pon", resourceFiles.sageGermlinePon(), "PON_COUNT", "PON_MAX");
        OutputFile ponAnnotationFile = ponAnnotation.apply(tumorSampleName).outputFile();
        result.addAll(ponAnnotation.bash(mappabilityAnnotationFile, ponAnnotationFile));
        SubStage ponFilter = new PonFilter(resourceFiles.version());
        OutputFile ponFilterFile = ponFilter.apply(tumorSampleName).outputFile();
        result.addAll(ponFilter.bash(mappabilityAnnotationFile, ponFilterFile));
        */

        return result;
    }
}
