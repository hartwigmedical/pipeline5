package com.hartwig.pipeline.calling.structural.gridss.command;

public class GripssHardFilterCommand extends GripssKtCommand
{
    public GripssHardFilterCommand(final String inputVcf, final String outputVcf) {
        super("com.hartwig.hmftools.gripsskt.GripssHardFilterApplicationKt",
                "-input_vcf",
                inputVcf,
                "-output_vcf",
                outputVcf);
    }
}
