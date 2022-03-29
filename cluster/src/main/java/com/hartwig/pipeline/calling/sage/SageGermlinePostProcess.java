package com.hartwig.pipeline.calling.sage;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.stages.SubStageInputOutput;

public class SageGermlinePostProcess extends SubStage {

    public static final String SAGE_GERMLINE_FILTERED = "sage.germline.filtered";
    private final SubStageInputOutput tumorSampleName;

    public SageGermlinePostProcess(final String referenceSampleName, final String tumorSampleName) {
        super(SAGE_GERMLINE_FILTERED, FileTypes.GZIPPED_VCF);
        this.tumorSampleName = SubStageInputOutput.empty(tumorSampleName);
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
        final List<BashCommand> result = Lists.newArrayList();

        SubStage passFilter = new PassFilter();

        OutputFile finalOutputFile = OutputFile.of(tumorSampleName.sampleName(), SAGE_GERMLINE_FILTERED, FileTypes.GZIPPED_VCF);
        result.addAll(passFilter.bash(input, finalOutputFile));

        /*
        SubStage mappabilityAnnotation =
                new MappabilityAnnotation(resourceFiles.mappabilityBed(), resourceFiles.mappabilityHDR());

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
        */
        return result;
    }
}
