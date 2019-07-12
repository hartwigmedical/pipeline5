package com.hartwig.pipeline.calling.structural.gridss.stage;

import static java.lang.String.format;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import com.hartwig.pipeline.execution.vm.unix.GunzipAndKeepArchiveCommand;
import com.hartwig.pipeline.execution.vm.unix.MvCommand;

import org.immutables.value.Value;

public class Filter {

    @Value.Immutable
    public interface FilterResult {
        List<BashCommand> commands();

        String fullVcf();

        String filteredVcf();
    }


    public FilterResult initialise(final String originalVcf, final String tumorSample) {
        String pathToGridssScripts =  VmDirectories.TOOLS + "/gridss-scripts/4.8.1";
        String unzippedOriginalVcf;
        Matcher matcher = Pattern.compile("(.+).gz$").matcher(originalVcf);
        if (matcher.matches()) {
            unzippedOriginalVcf = matcher.group(1);
        } else {
            throw new IllegalArgumentException(format("%s must have a .gz extension!", originalVcf));
        }

        String outputVcf = VmDirectories.outputFile(format("%s.gridss.somatic.vcf", tumorSample));
        String fullVcf = VmDirectories.outputFile(format("%s.gridss.somatic.full.vcf", tumorSample));
        String filteredVcf = VmDirectories.outputFile(format("%s.gridss.somatic.vcf.gz", tumorSample));
        String fullVcfCompressed = VmDirectories.outputFile(format("%s.gridss.somatic.full.vcf.gz", tumorSample));

        List<BashCommand> commands = new ArrayList<>();
        commands.add(new GunzipAndKeepArchiveCommand(originalVcf));
        commands.add(() -> format(
                "gunzip -c %s | awk ' { if (length($0) >= 4000) { gsub(\":0.00:\", "
                        + "\":0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                        + "00000000000000000000000000000000000000:\")} ; print $0  } ' > %s",
                originalVcf, unzippedOriginalVcf));      
        commands.add(() -> format("Rscript %s/gridss_somatic_filter.R -p %s -i %s -o %s -f %s -s %s",
                pathToGridssScripts,
                VmDirectories.RESOURCES,
                unzippedOriginalVcf,
                outputVcf,
                fullVcfCompressed,
                pathToGridssScripts));
        commands.add(new MvCommand(fullVcf + ".bgz", fullVcfCompressed));
        commands.add(new MvCommand(fullVcf + ".bgz.tbi", fullVcfCompressed + ".tbi"));
        commands.add(new MvCommand(outputVcf + ".bgz", filteredVcf));
        commands.add(new MvCommand(outputVcf + ".bgz.tbi", filteredVcf + ".tbi"));

        return ImmutableFilterResult.builder().commands(commands).fullVcf(fullVcfCompressed).filteredVcf(filteredVcf).build();
    }
}

