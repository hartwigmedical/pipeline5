package com.hartwig.pipeline.tertiary.pave;

import static com.hartwig.pipeline.metadata.InputMode.REFERENCE_ONLY;
import static com.hartwig.pipeline.metadata.InputMode.TUMOR_REFERENCE;

import java.util.List;

import com.hartwig.pipeline.calling.sage.SageGermlinePostProcess;
import com.hartwig.pipeline.calling.sage.SageOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.metadata.InputMode;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Stage;

public class PaveGermline extends Pave {
    public static final String NAMESPACE = "pave_germline";

    public PaveGermline(final ResourceFiles resourceFiles, SageOutput sageOutput, final PersistedDataset persistedDataset) {
        super(resourceFiles, sageOutput, persistedDataset, DataType.GERMLINE_VARIANTS_PAVE);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) { return referenceCommand(metadata, TUMOR_REFERENCE); }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) { return referenceCommand(metadata, REFERENCE_ONLY); }

    private List<BashCommand> referenceCommand(final SomaticRunMetadata metadata, final InputMode inputMode) {

        List<String> arguments = PaveArgumentBuilder.germline(
                resourceFiles,
                inputMode == REFERENCE_ONLY ? metadata.reference().sampleName() : metadata.tumor().sampleName(),
                vcfDownload.getLocalTargetPath());

        return paveCommand(metadata, arguments);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) { return Stage.disabled(); }

    @Override
    protected String outputFile(final SomaticRunMetadata metadata) {
        return String.format("%s.%s.%s.%s",
                metadata.sampleName(),
                SageGermlinePostProcess.SAGE_GERMLINE_FILTERED, PAVE_FILE_NAME,
                FileTypes.GZIPPED_VCF);
    }
}
