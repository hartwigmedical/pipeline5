package com.hartwig.pipeline.tertiary.pave;

import static com.hartwig.pipeline.tertiary.pave.PaveArguments.addTargetRegionsArguments;

import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;

@Namespace(PaveSomatic.NAMESPACE)
public class PaveSomatic extends Pave {
    private final Arguments arguments;

    public static final String NAMESPACE = "pave_somatic";
    private static final String PAVE_SOMATIC_FILE_ID = "pave.somatic";

    public PaveSomatic(final ResourceFiles resourceFiles, final SageOutput sageOutput, final PersistedDataset persistedDataset,
            final Arguments arguments) {
        super(resourceFiles, sageOutput, persistedDataset, DataType.SOMATIC_VARIANTS_PAVE);
        this.arguments = arguments;
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = PaveArguments.somatic(
                resourceFiles, metadata.tumor().sampleName(), vcfDownload.getLocalTargetPath(), outputFile(metadata));
        return paveCommand(metadata, arguments);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        List<String> arguments = PaveArguments.somatic(
                resourceFiles, metadata.tumor().sampleName(), vcfDownload.getLocalTargetPath(), outputFile(metadata));

        if (this.arguments.useTargetRegions()) {
            arguments.addAll(addTargetRegionsArguments(resourceFiles));
        }

        arguments.add("-write_pass_only");
        return paveCommand(metadata, arguments);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }

    @Override
    protected String outputFile(final SomaticRunMetadata metadata) {
        return String.format("%s.%s.%s",
                metadata.tumor().sampleName(), PAVE_SOMATIC_FILE_ID, FileTypes.GZIPPED_VCF);
    }
}
