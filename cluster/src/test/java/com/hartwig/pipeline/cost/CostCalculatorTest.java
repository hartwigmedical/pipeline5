package com.hartwig.pipeline.cost;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import com.google.api.services.cloudbilling.Cloudbilling;
import com.google.api.services.cloudbilling.model.ListSkusResponse;
import com.google.api.services.cloudbilling.model.Sku;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

public class CostCalculatorTest {

    private static final String REGION = "region";
    private Cloudbilling cloudbilling;
    private Cloudbilling.Services.Skus.List list;

    @Before
    public void setUp() throws Exception {
        cloudbilling = mock(Cloudbilling.class);
        Cloudbilling.Services services = mock(Cloudbilling.Services.class);
        final Cloudbilling.Services.Skus skus = mock(Cloudbilling.Services.Skus.class);
        when(cloudbilling.services()).thenReturn(services);
        when(cloudbilling.services().skus()).thenReturn(skus);
        list = mock(Cloudbilling.Services.Skus.List.class);
        when(skus.list(CostCalculator.COMPUTE_SERVICE)).thenReturn(list);
    }

    @Test
    public void returnsZeroIfComputeServiceNotFoundInGoogle() throws Exception {
        when(list.execute()).thenReturn(new ListSkusResponse().setSkus(new ArrayList<>()));
        CostCalculator victim = new CostCalculator(cloudbilling, REGION, skuMap -> singletonList(costOf(10.0)));
        double result = victim.calculate(PerformanceProfile.builder().build(), 1);
        assertThat(result).isZero();
    }

    @Test
    public void sumsCosts() throws Exception {
        when(list.execute()).thenReturn(new ListSkusResponse().setSkus(singletonList(new Sku().setName("sku")
                .setServiceRegions(singletonList(REGION)))));
        CostCalculator victim = new CostCalculator(cloudbilling, REGION, skuMap -> asList(costOf(10.0), costOf(5.0)));
        double result = victim.calculate(PerformanceProfile.builder().build(), 1);
        assertThat(result).isEqualTo(15.0);
    }

    @Test
    public void filtersSkusOutsideRegion() throws Exception {
        when(list.execute()).thenReturn(new ListSkusResponse().setSkus(singletonList(new Sku().setName("sku")
                .setServiceRegions(singletonList("US")))));
        CostCalculator victim = new CostCalculator(cloudbilling, REGION, skuMap -> asList(costOf(10.0), costOf(5.0)));
        double result = victim.calculate(PerformanceProfile.builder().build(), 1);
        assertThat(result).isZero();
    }

    @Test
    public void iteratesThroughAllPages() throws Exception {
        String next = "next";
        when(list.execute()).thenReturn(new ListSkusResponse().setNextPageToken(next)
                        .setSkus(singletonList(new Sku().setSkuId("sku1").setServiceRegions(singletonList(REGION)))),
                new ListSkusResponse().setSkus(singletonList(new Sku().setSkuId("sku2").setServiceRegions(singletonList(REGION)))));
        CostCalculator victim = new CostCalculator(cloudbilling, REGION, skuMap -> singletonList(costOf(10.0)));
        victim.calculate(PerformanceProfile.builder().build(), 1);
        verify(list, times(2)).execute();
    }

    @NotNull
    private static Cost costOf(final double cost) {
        return (performanceProfile, hours) -> cost;
    }
}