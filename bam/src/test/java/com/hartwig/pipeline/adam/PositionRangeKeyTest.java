package com.hartwig.pipeline.adam;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class PositionRangeKeyTest {

    @Test
    public void firstKeyWithinSecondKeyRange() {
        PositionRangeKey first = PositionRangeKey.of(2, 2);
        PositionRangeKey second = PositionRangeKey.of(1, 10);
        assertThat(first).isEqualTo(second);
    }

    @Test
    public void secondKeyWithinFirstKeyRange() {
        PositionRangeKey first = PositionRangeKey.of(1, 10);
        PositionRangeKey second = PositionRangeKey.of(2, 2);
        assertThat(first).isEqualTo(second);
    }
}