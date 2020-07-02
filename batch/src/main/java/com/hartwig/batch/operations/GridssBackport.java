package com.hartwig.batch.operations;

import static java.lang.String.format;

import static com.hartwig.pipeline.resource.ResourceNames.GRIDSS_CONFIG;
import static com.hartwig.pipeline.resource.ResourceNames.VIRUS_REFERENCE_GENOME;

import java.io.File;

import com.hartwig.batch.BatchOperation;
import com.hartwig.batch.input.InputBundle;
import com.hartwig.batch.input.InputFileDescriptor;
import com.hartwig.pipeline.ResultsDirectory;
import com.hartwig.pipeline.calling.command.BwaCommand;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.structural.gridss.command.AllocateEvidence;
import com.hartwig.pipeline.calling.structural.gridss.command.AnnotateInsertedSequence;
import com.hartwig.pipeline.calling.structural.gridss.command.SoftClipsToSplitReads;
import com.hartwig.pipeline.execution.vm.BashStartupScript;
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

    @Override
    public VirtualMachineJobDefinition execute(final InputBundle inputs, final RuntimeBucket runtimeBucket,
            final BashStartupScript startupScript, final RuntimeFiles executionFlags) {

        final ResourceFiles resourceFiles = ResourceFilesFactory.buildResourceFiles(RefGenomeVersion.HG37);
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
        final String newRawVcf = VmDirectories.outputFile(sample + "." + Versions.GRIDSS.replace(".", "_") + ".raw.vcf.gz");
        final String configurationFilePath = ResourceFiles.of(GRIDSS_CONFIG, "gridss.properties");
        startupScript.addCommand(new AllocateEvidence(emptyBam1,
                emptyBam2,
                newAssemblyBam,
                inputVcf.localDestination(),
                newRawVcf,
                resourceFiles.refGenomeFile(),
                configurationFilePath));

        // 7. Viral Annotation
        final String newViralAnnotatedVcf = VmDirectories.outputFile(sample + "." + Versions.GRIDSS.replace(".", "_") + ".viral.vcf.gz");
        String virusReferenceGenomePath = ResourceFiles.of(VIRUS_REFERENCE_GENOME, "human_virus.fa");
        startupScript.addCommand(AnnotateInsertedSequence.viralAnnotation(newRawVcf, newViralAnnotatedVcf, virusReferenceGenomePath));

        // 8. Repeat Masker
        final String outputVcf = VmDirectories.outputFile(sample + ".gridss.unfiltered.vcf.gz");
        startupScript.addCommand(AnnotateInsertedSequence.repeatMasker(newViralAnnotatedVcf,
                outputVcf,
                resourceFiles.refGenomeFile(),
                resourceFiles.gridssRepeatMaskerDbBed()));

        // 9. Upload output
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

}
