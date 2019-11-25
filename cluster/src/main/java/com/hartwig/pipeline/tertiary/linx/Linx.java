package com.hartwig.pipeline.tertiary.linx;

import static com.hartwig.pipeline.resource.ResourceNames.KNOWLEDGEBASES;
import static com.hartwig.pipeline.resource.ResourceNames.SV;

import java.util.Collections;
import java.util.List;

import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SomaticRunMetadata;
import com.hartwig.pipeline.report.EntireOutputComponent;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tertiary.purple.PurpleOutput;

public class Linx implements Stage<LinxOutput, SomaticRunMetadata> {

    public static final String NAMESPACE = "linx";
    private final InputDownload purpleOutputDirDownload;
    private final InputDownload purpleStructuralVcfDownload;

    public Linx(PurpleOutput purpleOutput) {
        purpleOutputDirDownload = new InputDownload(purpleOutput.outputDirectory());
        purpleStructuralVcfDownload = new InputDownload(purpleOutput.structuralVcf());
    }

    @Override
    public List<BashCommand> inputs() {
        return Collections.singletonList(purpleOutputDirDownload);
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SomaticRunMetadata metadata) {
        return Collections.singletonList(new LinxCommand(metadata.tumor().sampleName(),
                purpleStructuralVcfDownload.getLocalTargetPath(),
                purpleOutputDirDownload.getLocalTargetPath(),
                Resource.REFERENCE_GENOME_FASTA,
                VmDirectories.OUTPUT,
                Resource.of(SV, "fragile_sites_hmf.csv"),
                Resource.of(SV, "line_elements.csv"),
                Resource.of(SV, "heli_rep_origins.bed"),
                Resource.of(SV, "viral_host_ref.csv"),
                Resource.of(ResourceNames.ENSEMBL),
                Resource.of(KNOWLEDGEBASES, "knownFusionPairs.csv"),
                Resource.of(KNOWLEDGEBASES, "knownPromiscuousFive.csv"),
                Resource.of(KNOWLEDGEBASES, "knownPromiscuousThree.csv")));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.linx(bash, resultsDirectory);
    }

    @Override
    public LinxOutput output(final SomaticRunMetadata metadata, final PipelineStatus jobStatus, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return LinxOutput.builder()
                .status(jobStatus)
                .addReportComponents(new EntireOutputComponent(bucket, Folder.from(), NAMESPACE, resultsDirectory))
                .build();
    }

    @Override
    public LinxOutput skippedOutput(final SomaticRunMetadata metadata) {
        return LinxOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runTertiary() && !arguments.shallow();
    }
}
