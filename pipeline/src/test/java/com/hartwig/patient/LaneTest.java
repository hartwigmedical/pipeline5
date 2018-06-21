package com.hartwig.patient;

import static org.assertj.core.api.Assertions.assertThat;

import com.hartwig.testsupport.Lanes;

import org.junit.Test;

public class LaneTest {

    @Test
    public void readGroupIdConformsToStandard() {
        Lane victim = Lanes.emptyBuilder().name("sample_lane").flowCellId("flowcell").index("index").suffix("suffix").build();
        assertThat(victim.recordGroupId()).isEqualTo("sample_flowcell_index_lane_suffix");
    }
}