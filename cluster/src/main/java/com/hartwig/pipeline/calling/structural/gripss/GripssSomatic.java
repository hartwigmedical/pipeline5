package com.hartwig.pipeline.calling.structural.gripss;

import static com.hartwig.pipeline.datatypes.DataType.GRIPSS_SOMATIC_STRUCTURAL_VARIANTS;
import static com.hartwig.pipeline.datatypes.DataType.GRIPSS_SOMATIC_STRUCTURAL_VARIANTS_UNFILTERED;

import java.util.List;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.input.SomaticRunMetadata;
import com.hartwig.pipeline.output.OutputClassUtil;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;

@Namespace(GripssSomatic.GRIPSS_SOMATIC_NAMESPACE)
public class GripssSomatic extends Gripss {

    private final boolean useTargetRegions;

    public static final String GRIPSS_SOMATIC_NAMESPACE = "gripss_somatic";

    private static final String GRIPSS_SOMATIC_FILTERED = ".gripss.filtered.somatic.";
    private static final String GRIPSS_SOMATIC_UNFILTERED = ".gripss.somatic.";

    public GripssSomatic(final PersistedDataset persistedDataset, final ResourceFiles resourceFiles, final Arguments arguments) {
        super(persistedDataset, resourceFiles, GRIPSS_SOMATIC_NAMESPACE);
        useTargetRegions = arguments.useTargetRegions();
    }

    @Override
    public String outputClassTag() {
        return OutputClassUtil.getOutputClassTag(GripssOutput.class, "somatic");
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
    public DataType filteredDatatype() { return GRIPSS_SOMATIC_STRUCTURAL_VARIANTS; }

    @Override
    public DataType unfilteredDatatype() { return GRIPSS_SOMATIC_STRUCTURAL_VARIANTS_UNFILTERED; }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", metadata.tumor().sampleName()));
        arguments.add("-filter_sgls");
        arguments.add("-output_id somatic");
        arguments.addAll(commonArguments());

        if(useTargetRegions)
        {
            arguments.add("-hard_min_tumor_qual 200");
            arguments.add("-min_qual_break_point 1000");
            arguments.add("-min_qual_break_end 1000");
        }

        return formCommand(arguments);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }

}
