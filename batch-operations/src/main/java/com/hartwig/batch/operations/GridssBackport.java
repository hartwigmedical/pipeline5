package com.hartwig.batch.operations;

import static java.lang.String.format;

import java.io.File;
import java.util.Collections;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.OperationDescriptor;
import com.hartwig.batch.input.ImmutableInputFileDescriptor;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.AllocateEvidence;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.calling.structural.gridss.stage.GridssAnnotation;
import com.hartwig.pipeline.datatypes.FileTypes;
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
import com.hartwig.pipeline.stages.SubStageInputOutput;
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

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.HG19);
        final InputFileDescriptor template = inputs.get("set");
        final String set = inputs.get("set").inputValue();
        final String sample = inputs.get("tumor_sample").inputValue();
        final String bamFile = String.format("gs://hmf-gridss/assembly/%s/%s.assembly.bam.sv.bam", set, sample);
        final String vcfFile = String.format("gs://hmf-gridss/original/%s/%s.gridss.unfiltered.vcf.gz", set, sample);
        final InputFileDescriptor inputBam = ImmutableInputFileDescriptor.builder().from(template).inputValue(bamFile).build();
        final InputFileDescriptor inputBamIndex = inputBam.index();
        final InputFileDescriptor inputVcf = ImmutableInputFileDescriptor.builder().from(template).inputValue(vcfFile).build();
        final InputFileDescriptor inputVcfIndex = inputVcf.index();

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
        final OutputFile newRawVcf = OutputFile.of(sample, "gridss_" + Versions.GRIDSS.replace(".", "_") + ".raw", FileTypes.GZIPPED_VCF);
        startupScript.addCommand(new AllocateEvidence(emptyBam1,
                emptyBam2,
                newAssemblyBam,
                inputVcf.localDestination(),
                newRawVcf.path(),
                resourceFiles.refGenomeFile(),
                resourceFiles.gridssPropertiesFile()));

        // 7. Gridss Annotation
        final SubStageInputOutput annotation =
                new GridssAnnotation(resourceFiles, true).apply(SubStageInputOutput.of(sample, newRawVcf, Collections.emptyList()));
        startupScript.addCommands(annotation.bash());

        // 8. Archive targeted output
        final OutputFile unfilteredVcf = annotation.outputFile();
        final OutputFile unfilteredVcfIndex = unfilteredVcf.index(".tbi");
        final GoogleStorageLocation unfilteredVcfRemoteLocation = remoteUnfilteredVcfArchivePath(set, sample);
        final GoogleStorageLocation unfilteredVcfIndexRemoteLocation = index(unfilteredVcfRemoteLocation, ".tbi");
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

    public static GoogleStorageLocation index(GoogleStorageLocation template, String extension) {
        if (template.isDirectory()) {
            throw new IllegalArgumentException();
        }

        return GoogleStorageLocation.of(template.bucket(), template.path() + extension, template.isDirectory());
    }
}
