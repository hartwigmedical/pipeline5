package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import com.hartwig.pipeline.execution.vm.BashCommand;

public class BiocondaVariantAnnotationWorkaround implements BashCommand {

    private final String originalVcf;
    private final String unzippedOriginalVcf;

    public BiocondaVariantAnnotationWorkaround(final String originalVcf, final String unzippedOriginalVcf) {
        this.originalVcf = originalVcf;
        this.unzippedOriginalVcf = unzippedOriginalVcf;
    }

    @Override
    public String asBash() {
        return format(
                "gunzip -c %s | awk ' { if (length($0) >= 4000) { gsub(\":0.00:\", \":0.000000000000000000000000000000000000000000000000000"
                        + "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000:\")} ; print $0  "
                        + "} ' > %s",
                originalVcf, unzippedOriginalVcf);
    }
}
