package com.hartwig.pipeline.calling.structural.gripss;

import com.google.api.client.util.Lists;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.computeengine.input.SomaticRunMetadata;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.calling.structural.gridss.GridssOutput;
import com.hartwig.pipeline.datatypes.DataType;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.reruns.PersistedDataset;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.Namespace;
import com.hartwig.pipeline.stages.Stage;

import java.util.List;

import static com.hartwig.pipeline.datatypes.DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS;
import static com.hartwig.pipeline.datatypes.DataType.SOMATIC_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY;
import static java.lang.String.format;

@Namespace(GripssSomatic.GRIPSS_SOMATIC_NAMESPACE)
public class GripssSomatic extends Gripss {

    private final boolean useTargetRegions;

    public static final String GRIPSS_SOMATIC_NAMESPACE = "gripss_somatic";

    private static final String GRIPSS_SOMATIC_FILTERED = ".gripss.filtered.somatic.";
    private static final String GRIPSS_SOMATIC_UNFILTERED = ".gripss.somatic.";

    public GripssSomatic(final GridssOutput gridssOutput, final PersistedDataset persistedDataset, final ResourceFiles resourceFiles,
                         final Arguments arguments) {
        super(gridssOutput, persistedDataset, resourceFiles, GRIPSS_SOMATIC_NAMESPACE);
        useTargetRegions = arguments.useTargetRegions();
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
    public String filteredVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + GRIPSS_SOMATIC_FILTERED + FileTypes.GZIPPED_VCF;
    }

    @Override
    public String unfilteredVcf(final SomaticRunMetadata metadata) {
        return metadata.tumor().sampleName() + GRIPSS_SOMATIC_UNFILTERED + FileTypes.GZIPPED_VCF;
    }

    @Override
    public DataType filteredDatatype() {
        return SOMATIC_STRUCTURAL_VARIANTS_GRIPSS;
    }

    @Override
    public DataType unfilteredDatatype() {
        return SOMATIC_STRUCTURAL_VARIANTS_GRIPSS_RECOVERY;
    }

    @Override
    public List<BashCommand> tumorOnlyCommands(final SomaticRunMetadata metadata) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", metadata.tumor().sampleName()));
        arguments.add("-output_id somatic");
        arguments.addAll(commonArguments());

        if (useTargetRegions) {
            arguments.add("-hard_min_tumor_qual 200");
            arguments.add("-min_qual_break_point 1000");
            arguments.add("-min_qual_break_end 1000");
            arguments.add(format("-target_regions_bed %s", resourceFiles.targetRegionsBed()));
        }

        return formCommand(arguments);
    }

    @Override
    public List<BashCommand> referenceOnlyCommands(final SomaticRunMetadata metadata) {
        return Stage.disabled();
    }

}
