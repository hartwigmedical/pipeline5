package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.resource.RefGenomeVersion.V37;
import static com.hartwig.pipeline.tools.ExternalTool.SAMBAMBA;
import static com.hartwig.pipeline.tools.HmfTool.ESVEE;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

public class SvCalling extends SubStage {

    public static final String SV_PREP_CLASS_PATH = "com.hartwig.hmftools.esvee.prep.SvPrepApplication";
    public static final String ASSEMBLE_CLASS_PATH = "com.hartwig.hmftools.esvee.EsveeApplication";
    public static final String DEPTH_ANNOTATOR_CLASS_PATH = "com.hartwig.hmftools.esvee.depth.DepthAnnotator";
    public static final String CALLER_CLASS_PATH = "com.hartwig.hmftools.esvee.caller.CallerApplication";

    public static final String ESVEE_UNFILTERED_VCF = "esvee.unfiltered.vcf.gz";
    public static final String ESVEE_SOMATIC_VCF = "esvee.somatic.vcf.gz";
    public static final String ESVEE_GERMLINE_VCF = "esvee.germline.vcf.gz";

    private final ResourceFiles resourceFiles;
    private final List<SampleArgument> sampleArguments = new ArrayList<>();

    private enum SampleType
    {
        TUMOR,
        REFERENCE
    }

    private static class SampleArgument {
        private final SampleType Type;
        private final String SampleName;
        private final String BamPath;

        private SampleArgument(final SampleType type, final String sampleName, final String samplePath) {
            Type = type;
            SampleName = sampleName;
            BamPath = samplePath;
        }
    }

    public SvCalling(final ResourceFiles resourceFiles) {
        super("esvee", FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
    }

    public SvCalling tumorSample(final String tumorSampleName, final String tumorBamPath) {
        sampleArguments.add(new SampleArgument(SampleType.TUMOR, tumorSampleName, tumorBamPath));
        return this;
    }

    public SvCalling referenceSample(final String referenceSampleName, final String referenceSamplePath) {
        // ensure reference sample is processed first since this has a bearing on the order in the VCF where ref is first by convention
        sampleArguments.add(0, new SampleArgument(SampleType.REFERENCE, referenceSampleName, referenceSamplePath));
        return this;
    }

    private SampleArgument getSample(SampleType sampleType)
    {
        return sampleArguments.stream().filter(x -> x.Type == sampleType).findFirst().orElse(null);
    }

    private String mainSampleName()
    {
        SampleArgument tumorSample = getSample(SampleType.TUMOR);
        if(tumorSample != null)
            return tumorSample.SampleName;

        return sampleArguments.get(0).SampleName;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {

        List<BashCommand> commands = new ArrayList<>();

        commands.add(buildSvPrepCommand());
        commands.add(buildAssembleCommand());
        commands.add(buildDepthAnnotatorCommand());
        commands.add(buildCallerCommand());

        return commands;
    }

    private BashCommand buildSvPrepCommand() {

        String samplesString = sampleArguments.stream()
                .map(sampleArgument -> sampleArgument.SampleName)
                .collect(Collectors.joining(","));

        String bamFilesString = sampleArguments.stream()
                .map(sampleArgument -> sampleArgument.BamPath)
                .collect(Collectors.joining(","));

        List<String> arguments = new ArrayList<>();

        arguments.add(format("-sample %s", samplesString));
        arguments.add(format("-bam_files %s", bamFilesString));
        arguments.add(format("-blacklist_bed %s", resourceFiles.svPrepBlacklistBed()));
        arguments.add(format("-known_fusion_bed %s", resourceFiles.knownFusionPairBedpe()));
        arguments.add(format("-bamtool %s", SAMBAMBA.binaryPath()));
        arguments.add("-write_types \"JUNCTIONS;BAM;FRAGMENT_LENGTH_DIST\"");

        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(format("-threads %s", Bash.allCpus()));

        return JavaCommandFactory.javaClassCommand(ESVEE, SV_PREP_CLASS_PATH, arguments);
    }

    private String junctionsFile() {
        return format("%s/%s.esvee.prep.junctions.tsv", VmDirectories.OUTPUT, mainSampleName());
    }

    private String tumorPrepBam() {
        return format("%s/%s.esvee.prep.bam", VmDirectories.OUTPUT, getSample(SampleType.TUMOR).SampleName);
    }

    private String referencePrepBam() {
        return format("%s/%s.esvee.prep.bam", VmDirectories.OUTPUT, getSample(SampleType.REFERENCE).SampleName);
    }

    private BashCommand buildAssembleCommand() {

        List<String> arguments = new ArrayList<>();

        SampleArgument tumorSample = getSample(SampleType.TUMOR);
        if(tumorSample != null)
        {
            arguments.add(format("-tumor %s", tumorSample.SampleName));
            arguments.add(format("-tumor_bam %s", tumorPrepBam()));
        }

        SampleArgument referenceSample = getSample(SampleType.REFERENCE);
        if(referenceSample != null)
        {
            arguments.add(format("-reference %s", referenceSample.SampleName));
            arguments.add(format("-reference_bam %s", referencePrepBam()));
        }

        arguments.add(format("-junction_files %s", junctionsFile()));
        arguments.add("-write_types \"JUNC_ASSEMBLY;PHASED_ASSEMBLY;ALIGNMENTS;BREAKEND;VCF\"");

        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version()));

        if(resourceFiles.version().equals(V37))
        {
            arguments.add(format("-decoy_genome %s", resourceFiles.decoyGenome()));
        }

        arguments.add(format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(format("-threads %s", Bash.allCpus()));

        return JavaCommandFactory.javaClassCommand(ESVEE, ASSEMBLE_CLASS_PATH, arguments);
    }

    private String rawVcfFile() {
        return format("%s/%s.esvee.raw.vcf.gz", VmDirectories.OUTPUT, mainSampleName());
    }

    private BashCommand buildDepthAnnotatorCommand() {

        List<String> arguments = new ArrayList<>();

        String samplesString = sampleArguments.stream()
                .map(sampleArgument -> sampleArgument.SampleName)
                .collect(Collectors.joining(","));

        String bamFilesString = sampleArguments.stream()
                .map(sampleArgument -> sampleArgument.BamPath)
                .collect(Collectors.joining(","));

        arguments.add(format("-samples %s", samplesString));
        arguments.add(format("-bam_files %s", bamFilesString));

        arguments.add(format("-input_vcf %s", rawVcfFile()));

        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(format("-threads %s", Bash.allCpus()));

        return JavaCommandFactory.javaClassCommand(ESVEE, DEPTH_ANNOTATOR_CLASS_PATH, arguments);
    }

    private String refDepthVcfFile() {
        return format("%s/%s.esvee.ref_depth.vcf.gz", VmDirectories.OUTPUT, mainSampleName());
    }

    private BashCommand buildCallerCommand() {
        List<String> arguments = new ArrayList<>();

        arguments.add(format("-sample %s", mainSampleName()));

        SampleArgument referenceSample = getSample(SampleType.REFERENCE);
        if(referenceSample != null)
        {
            arguments.add(format("-reference %s", referenceSample.SampleName));
        }

        arguments.add(format("-input_vcf %s", refDepthVcfFile()));

        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version().toString()));

        arguments.add(format("-known_hotspot_file %s", resourceFiles.knownFusionPairBedpe()));
        arguments.add(format("-pon_sgl_file %s", resourceFiles.sglBreakendPon()));
        arguments.add(format("-pon_sv_file %s", resourceFiles.svBreakpointPon()));
        arguments.add(format("-repeat_mask_file %s", resourceFiles.repeatMaskerDb()));

        arguments.add(format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(format("-threads %s", Bash.allCpus()));

        return JavaCommandFactory.javaClassCommand(ESVEE, CALLER_CLASS_PATH, arguments);
    }
}
