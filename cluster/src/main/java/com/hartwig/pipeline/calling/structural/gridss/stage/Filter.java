package com.hartwig.pipeline.calling.structural.gridss.stage;

import com.hartwig.pipeline.execution.vm.BashCommand;
import com.hartwig.pipeline.execution.vm.VmDirectories;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.hartwig.pipeline.calling.structural.gridss.process.GridssCommon.pathToGridssScripts;
import static com.hartwig.pipeline.calling.structural.gridss.process.GridssCommon.ponDir;
import static java.lang.String.format;

public class Filter {

    @Value.Immutable
    public interface FilterResult {
        BashCommand command();
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
        String resultantVcf = VmDirectories.outputFile(format("%s.gridss.somatic.full.vcf.gz", tumorSample));

        List<String> lines = new ArrayList<>();
        lines.add(format("gunzip -c %s > %s", originalVcf, unzippedOriginalVcf));
        lines.add(format("Rscript %s/gridss_somatic_filter.R -p %s -i %s -o %s -f %s -s %s",
                pathToGridssScripts(), ponDir(), unzippedOriginalVcf, outputVcf, resultantVcf, pathToGridssScripts()));
        lines.add(format("mv %s.bgz %s", fullVcf, resultantVcf));
        lines.add(format("mv %s.bgz.tbi %s.tbi", fullVcf, resultantVcf));
        lines.add(format("mv %s.bgz %s", outputVcf, filteredVcf));
        lines.add(format("mv %s.bgz.tbi %s.tbi", outputVcf, filteredVcf));

        return ImmutableFilterResult.builder()
                .command(() -> lines.stream().collect(Collectors.joining("\n")))
                .fullVcf(resultantVcf)
                .filteredVcf(filteredVcf)
                .build();
    }
}

