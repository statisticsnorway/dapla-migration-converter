package no.ssb.rawdata.converter.app.migration.pubsub;

import com.google.pubsub.v1.PubsubMessage;
import io.micronaut.gcp.pubsub.annotation.PubSubClient;
import io.micronaut.gcp.pubsub.annotation.Topic;

@PubSubClient
public interface PubSubJobResponseClient {

    @Topic("migration-converter-job-responses")
    void send(PubsubMessage message);
}
