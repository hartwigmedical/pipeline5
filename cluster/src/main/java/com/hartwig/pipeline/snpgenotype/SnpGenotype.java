package com.hartwig.pipeline.snpgenotype;

import static java.lang.String.format;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.hartwig.pipeline.Arguments;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.alignment.AlignmentOutput;
import com.hartwig.pipeline.execution.PipelineStatus;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.InputDownload;
import com.hartwig.pipeline.execution.vm.ResourceDownload;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.metadata.SingleSampleRunMetadata;
import com.hartwig.pipeline.report.Folder;
import com.hartwig.pipeline.report.RunLogComponent;
import com.hartwig.pipeline.report.SingleFileComponent;
import com.hartwig.pipeline.report.StartupScriptComponent;
import com.hartwig.pipeline.resource.GATKDictAlias;
import com.hartwig.pipeline.resource.ReferenceGenomeAlias;
import com.hartwig.pipeline.resource.Resource;
import com.hartwig.pipeline.resource.ResourceNames;
import com.hartwig.pipeline.stages.Stage;
import com.hartwig.pipeline.storage.RuntimeBucket;

public class SnpGenotype implements Stage<SnpGenotypeOutput, SingleSampleRunMetadata> {

    public static final String NAMESPACE = "snp_genotype";

    private static final String OUTPUT_FILENAME = "snp_genotype_output.vcf";

    private final InputDownload bamDownload;

    public SnpGenotype(final AlignmentOutput alignmentOutput) {
        this.bamDownload = new InputDownload(alignmentOutput.finalBamLocation());
    }

    @Override
    public List<InputDownload> inputs() {
        return Collections.singletonList(bamDownload);
    }

    @Override
    public List<ResourceDownload> resources(final Storage storage, final String resourceBucket, final RuntimeBucket bucket) {
        return ImmutableList.of(ResourceDownload.from(bucket,
                new Resource(storage,
                        resourceBucket,
                        ResourceNames.REFERENCE_GENOME,
                        new ReferenceGenomeAlias().andThen(new GATKDictAlias()))),
                ResourceDownload.from(storage, resourceBucket, ResourceNames.GENOTYPE_SNPS, bucket));
    }

    @Override
    public String namespace() {
        return NAMESPACE;
    }

    @Override
    public List<BashCommand> commands(final SingleSampleRunMetadata metadata, final Map<String, ResourceDownload> resources) {
        return Collections.singletonList(new SnpGenotypeCommand(bamDownload.getLocalTargetPath(),
                resources.get(ResourceNames.REFERENCE_GENOME).find("fasta"),
                resources.get(ResourceNames.GENOTYPE_SNPS).find("26SNPtaq.vcf"),
                format("%s/%s", VmDirectories.OUTPUT, OUTPUT_FILENAME)));
    }

    @Override
    public VirtualMachineJobDefinition vmDefinition(final BashStartupScript bash, final ResultsDirectory resultsDirectory) {
        return VirtualMachineJobDefinition.snpGenotyping(bash, resultsDirectory);
    }

    @Override
    public SnpGenotypeOutput output(final SingleSampleRunMetadata metadata, final PipelineStatus status, final RuntimeBucket bucket,
            final ResultsDirectory resultsDirectory) {
        return SnpGenotypeOutput.builder()
                .status(status)
                .addReportComponents(new RunLogComponent(bucket, NAMESPACE, Folder.from(metadata), resultsDirectory))
                .addReportComponents(new StartupScriptComponent(bucket, NAMESPACE, Folder.from(metadata)))
                .addReportComponents(new SingleFileComponent(bucket,
                        NAMESPACE,
                        Folder.from(metadata),
                        OUTPUT_FILENAME,
                        OUTPUT_FILENAME,
                        resultsDirectory))
                .build();
    }

    @Override
    public SnpGenotypeOutput skippedOutput(final SingleSampleRunMetadata metadata) {
        return SnpGenotypeOutput.builder().status(PipelineStatus.SKIPPED).build();
    }

    @Override
    public boolean shouldRun(final Arguments arguments) {
        return arguments.runSnpGenotyper();
    }
}
