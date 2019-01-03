package com.hartwig.pipeline.janitor;

import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Janitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(Janitor.class);
    private static final String APPLICATION_NAME = "janitor";

    private final Set<CleanupTask> tasks = new HashSet<>();
    private final ScheduledExecutorService scheduledExecutorService;

    Janitor(final ScheduledExecutorService scheduledExecutorService) {
        this.scheduledExecutorService = scheduledExecutorService;
    }

    void register(CleanupTask task) {
        tasks.add(task);
    }

    private void sweep(Arguments arguments) {
        LOGGER.info("Janitor is sweeping....");
        tasks.forEach(task -> task.execute(arguments));
        LOGGER.info("Sweep complete");
    }

    void start(Arguments arguments) {
        scheduledExecutorService.scheduleAtFixedRate(() -> sweep(arguments), 0, arguments.intervalInSeconds(), TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        JanitorOptions.from(args).ifPresent(arguments -> {
            try {
                LOGGER.info("Starting the pipeline janitor with arguments [{}]", arguments);

                GoogleCredentials credentials =
                        GoogleCredentials.fromStream(new FileInputStream(arguments.privateKeyPath())).createScoped(DataprocScopes.all());
                Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(),
                        JacksonFactory.getDefaultInstance(),
                        new HttpCredentialsAdapter(credentials)).setApplicationName(APPLICATION_NAME).build();

                Janitor janitor = new Janitor(Executors.newSingleThreadScheduledExecutor());
                janitor.register(new ShutdownOrphanClusters(dataproc));
                janitor.start(arguments);

                Thread.currentThread().join();
            } catch (Exception e) {
                LOGGER.error("An unexpected issue arose while running the bootstrap. See the attached exception for more details.", e);
                System.exit(1);
            }
        });
    }
}
