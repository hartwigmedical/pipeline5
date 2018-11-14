package com.hartwig.pipeline.adam;

public class PositionRangeKey {

    private final long start;
    private final long end;

    private PositionRangeKey(final long start, final long end) {
        this.start = start;
        this.end = end;
    }

    static PositionRangeKey of(final long start, final long end) {
        return new PositionRangeKey(start, end);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final PositionRangeKey that = (PositionRangeKey) o;

        if (this.start >= that.start && this.end <= that.end) {
            return true;
        }
        //noinspection RedundantIfStatement
        if (that.start >= this.start && that.end <= this.end) {
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return 1;
    }
}
