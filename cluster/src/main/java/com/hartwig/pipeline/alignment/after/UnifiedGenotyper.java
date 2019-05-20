package com.hartwig.pipeline.alignment.after;

import com.hartwig.pipeline.execution.vm.GatkCommand;

import java.util.List;

import static java.util.Arrays.asList;

public class UnifiedGenotyper extends GatkCommand {
    private final List<String> designs = asList("26SNPtaq", "32SNPtaq", "59SNPtaq", "81SNPmip");

    public UnifiedGenotyper(String maxHeapSize, String analysisType, String... arguments) {
        super(maxHeapSize, analysisType, arguments);
    }
    /*
[% FOREACH pair IN sample_bams.pairs -%] <- not yet sure about this - think this should be handled by the fact that we're only calling against one sample at a time. but not sure about what the story
[% FOREACH design IN designs -%]
java -Xmx[% opt.POSTSTATS_MEM %]G -jar \
    "[% opt.GATK_PATH %]/GenomeAnalysisTK.jar" \
    -T UnifiedGenotyper \
    -R "[% opt.REF_GENOME %]" \
    -L "[% opt.OUTPUT_DIR %]/settings/slicing/[% design %].vcf" \
    --output_mode EMIT_ALL_SITES \
    -I "[% pair.value %]" \
    -o "[% dirs.${pair.key} %]/[% pair.key %]_[% design %].vcf"
[% END -%]
[% END %]

     */
}
