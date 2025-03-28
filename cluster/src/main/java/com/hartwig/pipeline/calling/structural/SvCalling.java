package com.hartwig.pipeline.calling.structural;

import static java.lang.String.format;

import static com.hartwig.pipeline.resource.RefGenomeVersion.V37;
import static com.hartwig.pipeline.tools.ExternalTool.SAMBAMBA;
import static com.hartwig.pipeline.tools.HmfTool.ESVEE;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.hartwig.computeengine.execution.vm.Bash;
import com.hartwig.computeengine.execution.vm.VmDirectories;
import com.hartwig.computeengine.execution.vm.command.BashCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.JavaCommandFactory;
import com.hartwig.pipeline.execution.OutputFile;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;

public class SvCalling extends SubStage {

    public static final String PREP_CLASS_PATH = "com.hartwig.hmftools.esvee.prep.PrepApplication";
    public static final String ASSEMBLE_CLASS_PATH = "com.hartwig.hmftools.esvee.assembly.AssemblyApplication";
    public static final String DEPTH_ANNOTATOR_CLASS_PATH = "com.hartwig.hmftools.esvee.depth.DepthAnnotator";
    public static final String CALLER_CLASS_PATH = "com.hartwig.hmftools.esvee.caller.CallerApplication";

    public static final String ESVEE_UNFILTERED_VCF = "esvee.unfiltered.vcf.gz";
    public static final String ESVEE_SOMATIC_VCF = "esvee.somatic.vcf.gz";
    public static final String ESVEE_GERMLINE_VCF = "esvee.germline.vcf.gz";
    public static final String ESVEE_PREP_BAM_FILE = "esvee.prep.bam";
    public static final String ESVEE_PREP_INDEX_FILE = "esvee.prep.bam.bai";
    public static final String ESVEE_PREP_JUNCTION_TSV = "esvee.prep.junction.tsv";
    public static final String ESVEE_FRAG_LENGTH_TSV = "esvee.prep.fragment_length.tsv";
    public static final String ESVEE_DISC_STATS_TSV = "esvee.prep.disc_stats.tsv";
    public static final String ESVEE_ASSEMBLY_TSV = "esvee.assembly.tsv";
    public static final String ESVEE_PHASED_ASSEMBLY_TSV = "esvee.phased_assembly.tsv";
    public static final String ESVEE_ALIGNMENT_TSV = "esvee.alignment.tsv";
    public static final String ESVEE_BREAKEND_TSV = "esvee.breakend.tsv";

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
        sampleArguments.add(new SampleArgument(SampleType.REFERENCE, referenceSampleName, referenceSamplePath));
        return this;
    }

    private boolean germlineOnly() { return getSample(SampleType.TUMOR) == null; }

    private List<SampleArgument> orderedSamples()
    {
        List<SampleArgument> samples = Lists.newArrayList();

        SampleArgument tumorSample = getSample(SampleType.TUMOR);

        if(tumorSample != null)
            samples.add(tumorSample);

        SampleArgument referenceSample = getSample(SampleType.REFERENCE);

        if(referenceSample != null)
            samples.add(referenceSample);

        return samples;
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

        // ensure tumor is passed in first since it's name is used for all prep output files
        List<SampleArgument> samples = orderedSamples();

        String samplesString = samples.stream()
                .map(sampleArgument -> sampleArgument.SampleName)
                .collect(Collectors.joining(","));

        String bamFilesString = samples.stream()
                .map(sampleArgument -> sampleArgument.BamPath)
                .collect(Collectors.joining(","));

        List<String> arguments = new ArrayList<>();

        arguments.add(format("-sample %s", samplesString));
        arguments.add(format("-bam_file %s", bamFilesString));
        arguments.add(format("-blacklist_bed %s", resourceFiles.svPrepBlacklistBed()));
        arguments.add(format("-known_fusion_bed %s", resourceFiles.knownFusionPairBedpe()));
        arguments.add(format("-bamtool %s", SAMBAMBA.binaryPath()));
        arguments.add("-write_types \"JUNCTIONS;BAM;FRAGMENT_LENGTH_DIST\"");

        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(format("-threads %s", Bash.allCpus()));
        // arguments.add("-log_debug");

        return JavaCommandFactory.javaClassCommand(ESVEE, PREP_CLASS_PATH, arguments);
    }

    private String junctionsFile() {
        return format("%s/%s.%s", VmDirectories.OUTPUT, mainSampleName(), ESVEE_PREP_JUNCTION_TSV);
    }

    private String tumorPrepBam() {
        return format("%s/%s.esvee.prep.bam", VmDirectories.OUTPUT, getSample(SampleType.TUMOR).SampleName);
    }

    private String referencePrepBam() {
        return format("%s/%s.esvee.prep.bam", VmDirectories.OUTPUT, getSample(SampleType.REFERENCE).SampleName);
    }

    private BashCommand buildAssembleCommand() {

        List<String> arguments = new ArrayList<>();

        if(!germlineOnly()) {

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
        }
        else {

            SampleArgument referenceSample = getSample(SampleType.REFERENCE);
            arguments.add(format("-tumor %s", referenceSample.SampleName));
            arguments.add(format("-tumor_bam %s", referencePrepBam()));
        }

        arguments.add(format("-junction_file %s", junctionsFile()));
        arguments.add("-write_types \"JUNC_ASSEMBLY;PHASED_ASSEMBLY;ALIGNMENT;BREAKEND;VCF\"");

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

        List<SampleArgument> samples = orderedSamples();

        String samplesString = samples.stream()
                .map(sampleArgument -> sampleArgument.SampleName)
                .collect(Collectors.joining(","));

        String bamFilesString = samples.stream()
                .map(sampleArgument -> sampleArgument.BamPath)
                .collect(Collectors.joining(","));

        arguments.add(format("-sample %s", samplesString));
        arguments.add(format("-bam_file %s", bamFilesString));

        arguments.add(format("-input_vcf %s", rawVcfFile()));

        arguments.add(format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(format("-unmap_regions %s", resourceFiles.unmapRegionsFile()));
        arguments.add(format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(format("-threads %s", Bash.allCpus()));

        return JavaCommandFactory.javaClassCommand(ESVEE, DEPTH_ANNOTATOR_CLASS_PATH, arguments);
    }

    private String refDepthVcfFile() {
        return format("%s/%s.esvee.ref_depth.vcf.gz", VmDirectories.OUTPUT, mainSampleName());
    }

    private BashCommand buildCallerCommand() {
        List<String> arguments = new ArrayList<>();

        if(!germlineOnly()){

            arguments.add(format("-sample %s", mainSampleName()));

            SampleArgument referenceSample = getSample(SampleType.REFERENCE);
            if(referenceSample != null)
            {
                arguments.add(format("-reference %s", referenceSample.SampleName));
            }
        }
        else {

            SampleArgument referenceSample = getSample(SampleType.REFERENCE);
            arguments.add(format("-reference %s", referenceSample.SampleName));
        }

        arguments.add(format("-input_vcf %s", refDepthVcfFile()));

        arguments.add(format("-ref_genome_version %s", resourceFiles.version().toString()));

        arguments.add(format("-known_hotspot_file %s", resourceFiles.knownFusionPairBedpe()));
        arguments.add(format("-pon_sgl_file %s", resourceFiles.sglBreakendPon()));
        arguments.add(format("-pon_sv_file %s", resourceFiles.svBreakpointPon()));
        arguments.add(format("-repeat_mask_file %s", resourceFiles.repeatMaskerDb()));

        arguments.add(format("-output_dir %s", VmDirectories.OUTPUT));

        return JavaCommandFactory.javaClassCommand(ESVEE, CALLER_CLASS_PATH, arguments);
    }
}
