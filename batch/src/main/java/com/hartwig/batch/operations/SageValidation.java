package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;
import java.util.List;

import com.google.common.collect.Lists;
import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.FinalSubStage;
import com.hartwig.pipeline.calling.SubStage;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.command.BcfToolsCommandListBuilder;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.calling.somatic.PonAnnotation;
import com.hartwig.pipeline.calling.somatic.SageCommandBuilder;
import com.hartwig.pipeline.calling.somatic.SageV2Application;
import com.hartwig.pipeline.calling.somatic.SageV2PonFilter;
import com.hartwig.pipeline.calling.somatic.SageV2PostProcess;
import com.hartwig.pipeline.calling.substages.SnpEff;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.UnzipToDirectoryCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class SageValidation implements BatchOperation {

    private static String localFilename(InputFileDescriptor remote) {
        return format("%s/%s", VmDirectories.INPUT, new File(remote.remoteFilename()).getName());
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.HG37);
        // Prepare SnpEff
        startupScript.addCommand(new UnzipToDirectoryCommand(VmDirectories.RESOURCES, resourceFiles.snpEffDb()));

        final InputFileDescriptor remoteTumorFile = inputs.get("tumor");
        final InputFileDescriptor remoteReferenceFile = inputs.get("reference");

        final InputFileDescriptor remoteTumorIndex = remoteTumorFile.index(".crai");
        final InputFileDescriptor remoteReferenceIndex = remoteReferenceFile.index(".crai");

        final String localTumorFile = localFilename(remoteTumorFile);
        final String localReferenceFile = localFilename(remoteReferenceFile);

        final String localTumorBam = localTumorFile.replace("cram", "bam");
        final String localReferenceBam = localReferenceFile.replace("cram", "bam");

        final String tumorSampleName = inputs.get("tumorSample").remoteFilename();
        final String referenceSampleName = inputs.get("referenceSample").remoteFilename();

        // Download latest jar file
        startupScript.addCommand(() -> format("gsutil -u hmf-crunch cp %s %s",
                "gs://batch-sage-validation/resources/sage.jar",
                "/opt/tools/sage/" + Versions.SAGE + "/sage.jar"));

        // Download tumor
        startupScript.addCommand(() -> remoteTumorFile.toCommandForm(localTumorFile));
        startupScript.addCommand(() -> remoteTumorIndex.toCommandForm(localFilename(remoteTumorIndex)));

        // Download normal
        startupScript.addCommand(() -> remoteReferenceFile.toCommandForm(localReferenceFile));
        startupScript.addCommand(() -> remoteReferenceIndex.toCommandForm(localFilename(remoteReferenceIndex)));

        final SageCommandBuilder sageCommandBuilder =
                new SageCommandBuilder(resourceFiles).addReference(referenceSampleName, localReferenceBam)
                        .addTumor(tumorSampleName, localTumorBam);

        if (inputs.contains("rna")) {
            final InputFileDescriptor remoteRnaBam = inputs.get("rna");
            final InputFileDescriptor remoteRnaBamIndex = remoteRnaBam.index(".bai");
            final String localRnaBam = localFilename(remoteRnaBam);

            // Download rna
            startupScript.addCommand(() -> remoteRnaBam.toCommandForm(localRnaBam));
            startupScript.addCommand(() -> remoteRnaBamIndex.toCommandForm(localFilename(remoteRnaBamIndex)));

            // Add to sage application
            sageCommandBuilder.addReference(referenceSampleName + "NA", localRnaBam);
        }

        // Convert to bam if necessary
        if (!localTumorFile.equals(localTumorBam)) {
            startupScript.addCommands(cramToBam(localTumorFile));
        }
        if (!localReferenceFile.equals(localReferenceBam)) {
            startupScript.addCommands(cramToBam(localReferenceFile));
        }

        // Run post processing
        final SageV2Application sageV2Application = new SageV2Application(sageCommandBuilder);
        final SubStageInputOutput postProcessing = sageV2Application.andThen(new CustomFilter())
                .andThen(new PonAnnotation("sage.pon", resourceFiles.sageGermlinePon(), "PON_COUNT", "PON_MAX"))
                .andThen(new SageV2PonFilter())
                .andThen(new SnpEff(ResourceFiles.SNPEFF_CONFIG, resourceFiles))
                .andThen(FinalSubStage.of(new SageV2PostProcess("hg19")))
                .apply(SubStageInputOutput.empty(tumorSampleName));
        startupScript.addCommands(postProcessing.bash());

        // Store output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "sage"), executionFlags));

        return VirtualMachineJobDefinition.sageCalling(startupScript, ResultsDirectory.defaultDirectory());
    }

    private List<BashCommand> cramToBam(String cram) {

        final String output = cram.replace("cram", "bam");
        final BashCommand toBam = new VersionedToolCommand("samtools",
                "samtools",
                Versions.SAMTOOLS,
                "view",
                "-o",
                output,
                "-O",
                "bam",
                "-@",
                Bash.allCpus(),
                cram);

        final BashCommand index =
                new VersionedToolCommand("samtools", "samtools", Versions.SAMTOOLS, "index", "-@", Bash.allCpus(), output);

        return Lists.newArrayList(toBam, index);
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("SageValidation", "Generate sage output for validation", OperationDescriptor.InputType.JSON);
    }

    static class CustomFilter extends SubStage {

        public CustomFilter() {
            super("sage.pass", OutputFile.GZIPPED_VCF);
        }

        @Override
        public List<BashCommand> bash(final OutputFile input, final OutputFile output) {
            return new BcfToolsCommandListBuilder(input.path(), output.path()).withIndex().includeHardPass().build();
        }
    }

}
