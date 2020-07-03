package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.pipeline.execution.vm.OutputFile.GZIPPED_VCF;
import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_CONFIG;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUS_REFERENCE_GENOME;

import java.io.File;
import java.util.Collections;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.SubStageInputOutput;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.AllocateEvidence;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssAnnotation;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.OutputUpload;
import com.hartwig.pipeline.execution.vm.RuntimeFiles;
import com.hartwig.pipeline.execution.vm.VirtualMachineJobDefinition;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.ExportPathCommand;
import com.hartwig.pipeline.resource.RefGenomeVersion;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.resource.ResourceFilesFactory;
import com.hartwig.pipeline.storage.GoogleStorageLocation;
import com.hartwig.pipeline.storage.RuntimeBucket;
import com.hartwig.pipeline.tools.Versions;

public class GridssBackport implements BatchOperation {

    public static GoogleStorageLocation remoteUnfilteredVcfArchivePath(final String set, final String sample) {
        return GoogleStorageLocation.of("hmf-gridss",
                "unfiltered" + File.separator + set + File.separator + sample + ".gridss.unfiltered.vcf.gz",
                false);
    }

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.HG37);
        final String set = inputs.get("set").inputValue();
        final String sample = inputs.get("sample").inputValue();
        final InputFileDescriptor inputBam = inputs.get("inputBam");
        final InputFileDescriptor inputBamIndex = inputBam.index(".bai");
        final InputFileDescriptor inputVcf = inputs.get("inputVcf");
        final InputFileDescriptor inputVcfIndex = inputVcf.index(".tbi");

        // 1. Set up paths
        startupScript.addCommand(new ExportPathCommand(new BwaCommand()));
        startupScript.addCommand(new ExportPathCommand(new SamtoolsCommand()));

        // 2. Download input files
        startupScript.addCommand(inputBam::copyToLocalDestinationCommand);
        startupScript.addCommand(inputBamIndex::copyToLocalDestinationCommand);
        startupScript.addCommand(inputVcf::copyToLocalDestinationCommand);
        startupScript.addCommand(inputVcfIndex::copyToLocalDestinationCommand);

        // 3. Get sample names
        startupScript.addCommand(() -> format("sampleNames=$(zgrep -m1 CHROM %s)", inputVcf.localDestination()));
        startupScript.addCommand(() -> "sample0=$(echo $sampleNames | cut -d \" \" -f 10)");
        startupScript.addCommand(() -> "sample1=$(echo $sampleNames | cut -d \" \" -f 11)");

        // 4. Create empty bams (and their working directories)
        final String emptyBam1 = String.format("%s/${%s}", VmDirectories.INPUT, "sample0");
        final String emptyBam1Working = workingDir(emptyBam1) + ".sv.bam";
        final String emptyBam2 = String.format("%s/${%s}", VmDirectories.INPUT, "sample1");
        final String emptyBam2Working = workingDir(emptyBam2) + ".sv.bam";
        startupScript.addCommand(() -> format("samtools view -H %s | samtools view -o %s", inputBam.localDestination(), emptyBam1));
        startupScript.addCommand(() -> format("samtools view -H %s | samtools view -o %s", inputBam.localDestination(), emptyBam2));
        startupScript.addCommand(() -> format("mkdir -p %s", dirname(emptyBam1Working)));
        startupScript.addCommand(() -> format("mkdir -p %s", dirname(emptyBam2Working)));
        startupScript.addCommand(() -> format("cp %s %s", emptyBam1, emptyBam1Working));
        startupScript.addCommand(() -> format("cp %s %s", emptyBam2, emptyBam2Working));

        // 5. SoftClipsToSplitReads
        final String newAssemblyBam = workingDir(inputBam.localDestination());
        startupScript.addCommand(() -> format("mkdir -p %s", dirname(newAssemblyBam)));
        startupScript.addCommand(new SoftClipsToSplitReads(inputBam.localDestination(), resourceFiles.refGenomeFile(), newAssemblyBam));

        // 6. Allocate Evidence
        final OutputFile newRawVcf = OutputFile.of(sample, "gridss_" + Versions.GRIDSS.replace(".", "_") + ".raw", GZIPPED_VCF);
        final String configurationFilePath = ResourceFiles.of(GRIDSS_CONFIG, "gridss.properties");
        startupScript.addCommand(new AllocateEvidence(emptyBam1,
                emptyBam2,
                newAssemblyBam,
                inputVcf.localDestination(),
                newRawVcf.path(),
                resourceFiles.refGenomeFile(),
                configurationFilePath));

        // 7. Gridss Annotation
        // TODO: This should be in resource files!
        String virusReferenceGenomePath = ResourceFiles.of(VIRUS_REFERENCE_GENOME, "human_virus.fa");
        final SubStageInputOutput annotation = new GridssAnnotation(resourceFiles, virusReferenceGenomePath).apply(SubStageInputOutput.of(
                sample,
                newRawVcf,
                Collections.emptyList()));
        startupScript.addCommands(annotation.bash());

        // 8. Archive targeted output
        final OutputFile unfilteredVcf = annotation.outputFile();
        final OutputFile unfilteredVcfIndex = unfilteredVcf.index(".tbi");
        final GoogleStorageLocation unfilteredVcfRemoteLocation = remoteUnfilteredVcfArchivePath(set, sample);
        final GoogleStorageLocation unfilteredVcfIndexRemoteLocation = index(unfilteredVcfRemoteLocation,".tbi");
        startupScript.addCommand(() -> unfilteredVcf.copyToRemoteLocation(unfilteredVcfRemoteLocation));
        startupScript.addCommand(() -> unfilteredVcfIndex.copyToRemoteLocation(unfilteredVcfIndexRemoteLocation));

        // 9. Upload all output
        startupScript.addCommand(new OutputUpload(GoogleStorageLocation.of(runtimeBucket.name(), "gridss"), executionFlags));
        return VirtualMachineJobDefinition.structuralCalling(startupScript, ResultsDirectory.defaultDirectory());
    }

    @Override
    public OperationDescriptor descriptor() {
        return OperationDescriptor.of("GridssBackport", "Bump gridss to latest version", OperationDescriptor.InputType.JSON);
    }

    private static String dirname(String filename) {
        return new File(filename).getParent();
    }

    private static String basename(String filename) {
        return new File(filename).getName();
    }

    private static String workingDir(String filename) {
        String dirname = dirname(filename);
        String basename = basename(filename);
        return dirname + "/" + basename + ".gridss.working/" + basename;
    }

    private static GoogleStorageLocation index(GoogleStorageLocation template, String extension) {
        if (template.isDirectory()) {
            throw new IllegalArgumentException();
        }

        return GoogleStorageLocation.of(template.bucket(), template.path() + extension, template.isDirectory());
    }
}
