package com.hartwig.pipeline.calling.structural.gripss;

import static com.hartwig.pipeline.datatypes.DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS;
import static com.hartwig.pipeline.datatypes.DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY;

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

@Namespace(GripssSomatic.SOMATIC_NAMESPACE)
public class GripssSomatic extends Gripss {

    public static final String SOMATIC_NAMESPACE = "gripss_somatic";

    private static final String GRIPSS_SOMATIC_FILTERED = ".gripss.filtered.somatic.";
    private static final String GRIPSS_SOMATIC_UNFILTERED = ".gripss.somatic.";

    public GripssSomatic(final GridssOutput gridssOutput, final PersistedDataset persistedDataset, final ResourceFiles resourceFiles) {
        super(gridssOutput, persistedDataset, resourceFiles, SOMATIC_NAMESPACE);
    }

    @Override
    public List<BashCommand> tumorReferenceCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", metadata.tumor().sampleName()));
        arguments.add(String.format("-reference %s", metadata.reference().sampleName()));
        arguments.add("-output_id somatic");
        arguments.addAll(commonArguments());

        return formCommand(arguments);
    }

    @Override
    public String filteredVcf(final SomaticRunMetadata metadata)
    {
        return metadata.tumor().sampleName() + GRIPSS_SOMATIC_FILTERED + FileTypes.GZIPPED_VCF;
    }

    @Override
    public String unfilteredVcf(final SomaticRunMetadata metadata)
    {
        return metadata.tumor().sampleName() + GRIPSS_SOMATIC_UNFILTERED + FileTypes.GZIPPED_VCF;
    }

    @Override
    public DataType filteredDatatype() { return SOMATIC_STRUCTURAL_VARIANTS_GRIPSS; }

    @Override
    public DataType unfilteredDatatype() { return SOMATIC_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY; }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", metadata.tumor().sampleName()));
        arguments.add("-filter_sgls");
        arguments.add("-output_id somatic");
        arguments.addAll(commonArguments());

        return formCommand(arguments);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }

}
