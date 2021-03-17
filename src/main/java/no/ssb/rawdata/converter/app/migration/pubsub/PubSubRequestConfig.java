package no.ssb.rawdata.converter.app.migration.pubsub;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

@ConfigurationProperties("gcp.pubsub.subscriber.migration-converter-job-requests")
@Data
public class PubSubRequestConfig {

    String topic;
    String subscription;
}
