package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.MkDirCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hartwig.pipeline.calling.structural.gridss.GridssCommon.pathToGridssScripts;
import static com.hartwig.pipeline.calling.structural.gridss.GridssCommon.ponDir;
import static java.lang.String.format;

public class Filter {

    @Value.Immutable
    public interface FilterResult {
        List<BashCommand> commands();
        String fullVcf();
        String filteredVcf();
    }

    public FilterResult initialise(String originalVcf, String tumorSample) {
        String unzippedOriginalVcf;
        Matcher matcher = Pattern.compile("(.+).gz$").matcher(originalVcf);
        if (matcher.matches()) {
            unzippedOriginalVcf = matcher.group(1);
        } else {
            throw new IllegalArgumentException(format("%s must have a .gz extension!"));
        }

        String outputVcf = VmDirectories.outputFile(format("%s.gridss.somatic.vcf", tumorSample));
        String fullVcf = VmDirectories.outputFile(format("%s.gridss.somatic.full.vcf", tumorSample));
        String filteredVcf = VmDirectories.outputFile(format("%s.gridss.somatic.vcf.gz", tumorSample));
        String fullVcfCompressed = VmDirectories.outputFile(format("%s.gridss.somatic.full.vcf.gz", tumorSample));

        List<BashCommand> commands = new ArrayList<>();
        commands.add(new MkDirCommand(format("%s/gridss_pon", VmDirectories.RESOURCES)));
        commands.add(() -> format("gsutil cp gs://common-resources/gridss_pon/* %s/gridss_pon", VmDirectories.RESOURCES));
        commands.add(() -> format("gunzip -kd %s", originalVcf));
        commands.add(() -> format("Rscript %s/gridss_somatic_filter.R -p %s -i %s -o %s -f %s -s %s",
                pathToGridssScripts(),
                ponDir(),
                unzippedOriginalVcf,
                outputVcf,
                fullVcfCompressed,
                pathToGridssScripts()));
        commands.add(() -> format("mv %s.bgz %s", fullVcf, fullVcfCompressed));
        commands.add(() -> format("mv %s.bgz.tbi %s.tbi", fullVcf, fullVcfCompressed));
        commands.add(() -> format("mv %s.bgz %s", outputVcf, filteredVcf));
        commands.add(() -> format("mv %s.bgz.tbi %s.tbi", outputVcf, filteredVcf));

        return ImmutableFilterResult.builder()
                .commands(commands)
                .fullVcf(fullVcfCompressed)
                .filteredVcf(filteredVcf)
                .build();
    }
}

