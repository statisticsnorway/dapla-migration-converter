package no.ssb.rawdata.converter.app.migration.pubsub;

import io.micronaut.gcp.pubsub.annotation.PubSubClient;
import io.micronaut.gcp.pubsub.annotation.Subscription;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;
import no.ssb.rawdata.converter.core.job.ConverterJobConfigFactory;
import no.ssb.rawdata.converter.core.job.ConverterJobScheduler;
import no.ssb.rawdata.converter.util.Json;

@PubSubClient
public class PubSubJobPublisher {

    final ConverterJobScheduler jobScheduler;
    final ConverterJobConfigFactory jobConfigFactory;

    public PubSubJobPublisher(ConverterJobScheduler jobScheduler, ConverterJobConfigFactory jobConfigFactory) {
        this.jobScheduler = jobScheduler;
        this.jobConfigFactory = jobConfigFactory;
    }

    @Subscription("migration-converter-job-requests")
    public void onMessage(ConverterJobConfig converterJobConfig) {
        System.out.printf("ConverterJob Received: %s%n", Json.prettyFrom(converterJobConfig));
    }
}
