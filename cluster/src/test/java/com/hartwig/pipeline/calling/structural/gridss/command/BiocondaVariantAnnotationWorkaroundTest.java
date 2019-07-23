package com.hartwig.pipeline.calling.structural.gridss.command;

import static java.lang.String.format;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class BiocondaVariantAnnotationWorkaroundTest {
    @Test
    public void shouldGenerateCommandProperly() {
        String unzippedOriginalVcf = "/some/directory/original.vcf";
        String originalVcf = unzippedOriginalVcf + ".gz";
        BiocondaVariantAnnotationWorkaround victim = new BiocondaVariantAnnotationWorkaround(originalVcf, unzippedOriginalVcf);

        String code = victim.asBash();
        String start = format("gunzip -c %s | awk ' { if (length($0) >= 4000) { gsub(\":0.00:\", \":0.", originalVcf);
        assertThat(code).startsWith(start);
        code = code.replace(start, "");
        String end = format(":\")} ; print $0  } ' > %s", unzippedOriginalVcf);
        assertThat(code).endsWith(end);
        code = code.replace(end, "");
        assertThat(code.matches("0{145}")).isTrue();
    }
}