package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import com.google.api.client.util.Lists;
import com.hartwig.pipeline.calling.command.SamtoolsCommand;
import com.hartwig.pipeline.calling.command.VersionedToolCommand;
import com.hartwig.pipeline.datatypes.FileTypes;
import com.hartwig.pipeline.execution.vm.Bash;
import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.OutputFile;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.java.JavaClassCommand;
import com.hartwig.pipeline.execution.vm.java.JavaJarCommand;
import com.hartwig.pipeline.resource.ResourceFiles;
import com.hartwig.pipeline.stages.SubStage;
import com.hartwig.pipeline.tools.Versions;

public class Driver extends SubStage {

    private static final String GRIDSS_SCRIPT = "gridss.run.sh";
    private static final String SV_PREP = "sv-prep";
    private static final String SV_PREP_JAR = "sv-prep.jar";

    private final ResourceFiles resourceFiles;
    private final List<SampleArgument> sampleArguments = Lists.newArrayList();

    private static final String MAX_HEAP = "48G";
    private static final int GRIDSS_THREADS = 10;

    private enum SampleType
    {
        TUMOR,
        REFERENCE
    }

    public Driver(final ResourceFiles resourceFiles) {
        super("gridss.driver", FileTypes.GZIPPED_VCF);
        this.resourceFiles = resourceFiles;
    }

    public Driver tumorSample(final String tumorSampleName, final String tumorBamPath) {
        sampleArguments.add(new SampleArgument(SampleType.TUMOR, tumorSampleName, tumorBamPath));
        return this;
    }

    public Driver referenceSample(final String referenceSampleName, final String referenceSamplePath) {
        // ensure reference sample is processed first since this has a bearing on the order in the VCF where ref is first by convention
        sampleArguments.add(0, new SampleArgument(SampleType.REFERENCE, referenceSampleName, referenceSamplePath));
        return this;
    }

    @Override
    public List<BashCommand> bash(final OutputFile input, final OutputFile output) {

        List<BashCommand> commands = Lists.newArrayList();

        // run tumor first to establish junctions for the ref
        String tumorJunctionsFile = null;
        SampleArgument tumorSample = sampleArguments.stream().filter(x -> x.Type == SampleType.TUMOR).findFirst().orElse(null);
        if(tumorSample != null)
        {
            addSvPrepCommands(commands, tumorSample, null);
            tumorJunctionsFile = format("%s/%s.sv_prep.junctions.csv", VmDirectories.OUTPUT, tumorSample.SampleName);
        }

        SampleArgument refSample = sampleArguments.stream().filter(x -> x.Type == SampleType.REFERENCE).findFirst().orElse(null);
        if(refSample != null)
        {
            addSvPrepCommands(commands, refSample, tumorJunctionsFile);
        }

        // call Gridss on these BAMs
        final String gridssVcf = String.format("%s/%s.gridss.vcf.gz", VmDirectories.OUTPUT, mainSampleName());

        commands.add(buildGridsCommand(gridssVcf));

        // run SvPrep again to set reference depth
        commands.add(buildRefDepthCommand(gridssVcf, output));

        return commands;
    }

    private void addSvPrepCommands(final List<BashCommand> commands, final SampleArgument sampleArgument, final String junctionsFile)
    {
        // run SvPrep on tumor and/or reference
        commands.add(buildSvPrepCommand(sampleArgument, junctionsFile));

        String svPrepBam = format("%s/%s.sv_prep.bam", VmDirectories.OUTPUT, sampleArgument.SampleName);
        sampleArgument.SvPrepBamPath = format("%s/%s.sv_prep.sorted.bam", VmDirectories.OUTPUT, sampleArgument.SampleName);

        // sort
        StringJoiner sortArguments = new StringJoiner(" ");
        sortArguments.add("sort");
        sortArguments.add(format("-@ %s", Bash.allCpus()));
        sortArguments.add("-m 2G -T tmp");
        sortArguments.add(format("-O bam %s", svPrepBam));
        sortArguments.add(format("-o %s", sampleArgument.SvPrepBamPath));
        commands.add(new SamtoolsCommand(sortArguments.toString()));

        // and index
        String indexArgs = format("index -@ %s %s", Bash.allCpus(), sampleArgument.SvPrepBamPath);
        commands.add(new SamtoolsCommand(indexArgs));
    }

    private BashCommand buildSvPrepCommand(final SampleArgument sampleArgument, final String tumorJunctionsFile) {

        List<String> arguments = Lists.newArrayList();

        arguments.add(String.format("-sample %s", sampleArgument.SampleName));
        arguments.add(String.format("-bam_file %s", sampleArgument.BamPath));
        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        arguments.add(String.format("-blacklist_bed %s", resourceFiles.svPrepBlacklistBed()));
        arguments.add(String.format("-known_fusion_bed %s", resourceFiles.knownFusionPairBedpe()));

        if(tumorJunctionsFile != null)
            arguments.add(String.format("-existing_junction_file %s", tumorJunctionsFile));

        arguments.add("-write_types \"JUNCTIONS;BAM;FRAGMENT_LENGTH_DIST\"");
        arguments.add(String.format("-output_dir %s", VmDirectories.OUTPUT));
        arguments.add(format("-threads %s", Bash.allCpus()));

        // arguments.add("-log_level INFO");

        return new JavaJarCommand(SV_PREP, Versions.SV_PREP, SV_PREP_JAR, MAX_HEAP, arguments);
    }

    private BashCommand buildGridsCommand(final String gridssVcf) {

        // annotate reference depth for each variant called by Gridss
        StringJoiner arguments = new StringJoiner(" ");

        arguments.add("--steps all");
        arguments.add(String.format("--output %s", gridssVcf));
        arguments.add(String.format("--workingdir %s", VmDirectories.OUTPUT)); // or write to a subdirectory?
        arguments.add(String.format("--reference %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("--jar %s", GridssJar.path()));
        arguments.add(String.format("--blacklist %s", resourceFiles.gridssBlacklistBed()));
        arguments.add(String.format("--configuration %s", resourceFiles.gridssPropertiesFile()));

        String sampleNames = sampleArguments.stream().map(x -> x.SampleName).collect(Collectors.joining(","));
        arguments.add(String.format("--labels %s", sampleNames));

        String fullBams = sampleArguments.stream().map(x -> x.BamPath).collect(Collectors.joining(","));
        arguments.add(String.format("--bams %s", fullBams));

        String svPrepBams = sampleArguments.stream().map(x -> x.SvPrepBamPath).collect(Collectors.joining(","));
        arguments.add(String.format("--filtered_bams %s", svPrepBams));

        arguments.add(String.format("--jvmheap %s", MAX_HEAP));
        arguments.add(String.format("--threads %d", GRIDSS_THREADS));

        return new VersionedToolCommand(SV_PREP, GRIDSS_SCRIPT, Versions.SV_PREP, arguments.toString());
    }

    private BashCommand buildRefDepthCommand(final String gridssVcf, final OutputFile output) {

        // run SvPrep on the output again to populate reference depth
        // final String gridssDepthVcf = String.format("%s/%s.gridss.unfiltered.vcf.gz", VmDirectories.OUTPUT, mainSampleName());

        final StringJoiner arguments = new StringJoiner(" ");
        arguments.add(String.format("-input_vcf %s", gridssVcf));
        arguments.add(String.format("-output_vcf %s", output.path()));

        String sampleNames = sampleArguments.stream().map(x -> x.SampleName).collect(Collectors.joining(","));
        arguments.add(String.format("-samples %s", sampleNames));

        String fullBams = sampleArguments.stream().map(x -> x.BamPath).collect(Collectors.joining(","));
        arguments.add(String.format("-bam_files %s", fullBams));

        arguments.add(String.format("-ref_genome %s", resourceFiles.refGenomeFile()));
        arguments.add(String.format("-ref_genome_version %s", resourceFiles.version().toString()));
        // arguments.add("-log_level DEBUG");
        arguments.add(format("-threads %s", Bash.allCpus()));

        return new JavaClassCommand(
                SV_PREP, Versions.SV_PREP, SV_PREP_JAR, "com.hartwig.hmftools.svprep.depth.DepthAnnotator",
                MAX_HEAP, arguments.toString());
    }

    private String mainSampleName() {
        SampleArgument tumorSample = sampleArguments.stream().filter(x -> x.Type == SampleType.TUMOR).findFirst().orElse(null);
        if(tumorSample != null)
            return tumorSample.SampleName;

        return sampleArguments.get(0).SampleName;
    }

    private static class SampleArgument { // implements Comparable<SampleArgument>
        private final SampleType Type;
        private final String SampleName;
        private final String BamPath;
        private String SvPrepBamPath;

        private SampleArgument(final SampleType type, final String sampleName, final String samplePath) {
            Type = type;
            SampleName = sampleName;
            BamPath = samplePath;
            SvPrepBamPath = "";
        }
    }
}
