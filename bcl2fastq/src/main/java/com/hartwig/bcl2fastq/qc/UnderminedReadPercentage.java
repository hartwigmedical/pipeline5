package com.hartwig.bcl2fastq.qc;

public class UnderminedReadPercentage implements FlowcellQualityCheck {
    private final int maxUnderminedYieldPercentage;

    UnderminedReadPercentage(final int maxUnderminedYieldPercentage) {
        this.maxUnderminedYieldPercentage = maxUnderminedYieldPercentage;
    }

    @Override
    public QualityControlResult apply(final Stats stats, final String log) {
        long totalYield = stats.conversionResults().stream().flatMap(c -> c.demuxResults().stream()).mapToLong(SampleStats::yield).sum();
        long undetermined = stats.conversionResults().stream().map(LaneStats::undetermined).mapToLong(UndeterminedStats::yield).sum();
        int percUndeterminedYield = (int) ((double) undetermined / (double) totalYield * 100);
        if (percUndeterminedYield > maxUnderminedYieldPercentage) {
            return QualityControlResult.of(name(), false);
        }
        return QualityControlResult.of(name(), true);
    }

    private String name() {
        return String.format("Undermined Percentage greater than [%s]", maxUnderminedYieldPercentage);
    }
}
