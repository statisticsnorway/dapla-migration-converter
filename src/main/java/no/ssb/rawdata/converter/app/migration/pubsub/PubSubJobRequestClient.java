package no.ssb.rawdata.converter.app.migration.pubsub;

import io.micronaut.gcp.pubsub.annotation.PubSubClient;
import io.micronaut.gcp.pubsub.annotation.Topic;
import no.ssb.rawdata.converter.core.job.ConverterJobConfig;

@PubSubClient
public interface PubSubJobRequestClient {

    @Topic("migration-converter-job-requests")
    void send(ConverterJobConfig converterJobConfig);
}
