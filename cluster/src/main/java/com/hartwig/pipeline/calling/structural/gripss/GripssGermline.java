package com.hartwig.pipeline.calling.structural.gripss;

import static com.hartwig.pipeline.datatypes.DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS;
import static com.hartwig.pipeline.datatypes.DataType.GERMLINE_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;

@Namespace(GripssGermline.GRIPSS_GERMLINE_NAMESPACE)
public class GripssGermline extends Gripss {

    public static final String GRIPSS_GERMLINE_NAMESPACE = "gripss_germline";

    private static final String GRIPSS_GERMLINE_FILTERED = ".gripss.filtered.germline.";
    private static final String GRIPSS_GERMLINE_UNFILTERED = ".gripss.germline.";

    public GripssGermline(final GridssOutput gridssOutput, final PersistedDataset persistedDataset,
            final ResourceFiles resourceFiles) {
        super(gridssOutput, persistedDataset, resourceFiles, GRIPSS_GERMLINE_NAMESPACE);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {

        return buildCommand(metadata);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {

        return buildCommand(metadata);
    }

    private List<BashCommand> buildCommand(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", metadata.reference().sampleName()));
        arguments.add("-output_id germline");
        arguments.addAll(commonArguments());

        return formCommand(arguments);
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }

    @Override
    public String filteredVcf(final SomaticRunMetadata metadata) {
        return metadata.reference().sampleName() + GRIPSS_GERMLINE_FILTERED + FileTypes.GZIPPED_VCF;
    }

    @Override
    public String unfilteredVcf(final SomaticRunMetadata metadata) {
        return metadata.reference().sampleName() + GRIPSS_GERMLINE_UNFILTERED + FileTypes.GZIPPED_VCF;
    }

    @Override
    public DataType filteredDatatype() { return GERMLINE_STRUCTURAL_VARIANTS_GRIPSS; }

    @Override
    public DataType unfilteredDatatype() { return GERMLINE_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY; }
}
