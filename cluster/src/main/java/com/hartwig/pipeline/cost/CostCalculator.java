package com.hartwig.pipeline.cost;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudbilling.Cloudbilling;
import com.google.api.services.cloudbilling.model.ListSkusResponse;
import com.google.api.services.cloudbilling.model.Sku;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.hartwig.pipeline.performance.PerformanceProfile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CostCalculator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CostCalculator.class);
    static final String COMPUTE_SERVICE = "services/6F81-5844-456A";

    private final Cloudbilling cloudbilling;
    private final String region;
    private final Costs costs;

    public CostCalculator(GoogleCredentials credentials, final String region, final Costs costs) {
        this(new Cloudbilling.Builder(new NetHttpTransport(),
                JacksonFactory.getDefaultInstance(),
                new HttpCredentialsAdapter(credentials)).build(), region, costs);
    }

    CostCalculator(final Cloudbilling cloudbilling, final String region, final Costs costs) {
        this.cloudbilling = cloudbilling;
        this.region = region;
        this.costs = costs;
    }

    public double calculate(PerformanceProfile profile, double hours) {
        try {
            Map<String, Sku> skus = getSkus(COMPUTE_SERVICE);
            if (skus.isEmpty()) {
                LOGGER.warn("No SKUs found for service [{}]. Could be the service name is incorrect of something has changed in the Google "
                        + "Billing Catalog API. Continuing with pipeline run but cost will not be recorded.", COMPUTE_SERVICE);
                return 0;
            }
            return costs.list(skus).stream().mapToDouble(cost -> cost.calculate(profile, hours)).sum();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, Sku> getSkus(final String service) throws IOException {
        List<Sku> skus = new ArrayList<>();
        Cloudbilling.Services.Skus.List list = cloudbilling.services().skus().list(service);
        ListSkusResponse response = list.execute();
        skus.addAll(response.getSkus());
        while (response.getNextPageToken() != null && !response.getNextPageToken().trim().isEmpty()) {
            list.setPageToken(response.getNextPageToken());
            response = list.execute();
            skus.addAll(response.getSkus());
        }
        return skus.stream()
                .filter(sku -> sku.getServiceRegions().contains(region))
                .collect(Collectors.toMap(Sku::getSkuId, Function.identity()));
    }
}
