package com.hartwig.pipeline.calling.somatic;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.calling.substages.SnpEff;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.stages.SubStageInputOutput;

public class SagePostProcessGermline extends SubStage {

    public static final String SAGE_GERMLINE_FILTERED = "sage.germline.filtered";
    private final ResourceFiles resourceFiles;
    private final SubStageInputOutput tumorSampleName;
    private final SubStage selectSamples;


    public SagePostProcessGermline(final String referenceSampleName, final String tumorSampleName, final ResourceFiles resourceFiles) {
        super(SAGE_GERMLINE_FILTERED, FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
        this.tumorSampleName = SubStageInputOutput.empty(tumorSampleName);
        this.selectSamples = new SelectSamples(referenceSampleName, tumorSampleName);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        final List<BashCommand> result = Lists.newArrayList();

        SubStage passFilter = new PassFilter();
        SubStage snpEff = new SnpEff(resourceFiles);
        SubStage mappabilityAnnotation =
                new MappabilityAnnotation(resourceFiles.out150Mappability(), resourceFiles.mappabilityHDR());

        SubStage clinvarAnnotation = new ClinvarAnnotation(resourceFiles);
        SubStage blacklistBedAnnotation = new BlacklistBedAnnotation(resourceFiles);
        SubStage blacklistVcfAnnotation = new BlacklistVcfAnnotation(resourceFiles);

        OutputFile passFilterFile = passFilter.apply(tumorSampleName).outputFile();
        OutputFile selectedSampleFile = selectSamples.apply(tumorSampleName).outputFile();
        OutputFile mappabilityAnnotationFile = mappabilityAnnotation.apply(tumorSampleName).outputFile();
        OutputFile clinvarFile = clinvarAnnotation.apply(tumorSampleName).outputFile();
        OutputFile blacklistBedFile = blacklistBedAnnotation.apply(tumorSampleName).outputFile();
        OutputFile blacklistVcfFile = blacklistVcfAnnotation.apply(tumorSampleName).outputFile();

        result.addAll(passFilter.bash(input, passFilterFile));
        result.addAll(selectSamples.bash(passFilterFile, selectedSampleFile));
        result.addAll(mappabilityAnnotation.bash(selectedSampleFile, mappabilityAnnotationFile));
        result.addAll(clinvarAnnotation.bash(mappabilityAnnotationFile, clinvarFile));
        result.addAll(blacklistBedAnnotation.bash(clinvarFile, blacklistBedFile));
        result.addAll(blacklistVcfAnnotation.bash(blacklistBedFile, blacklistVcfFile));
        result.addAll(snpEff.bash(blacklistVcfFile, output));
        return result;
    }
}
