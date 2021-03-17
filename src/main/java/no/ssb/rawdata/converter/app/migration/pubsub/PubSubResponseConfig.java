package no.ssb.rawdata.converter.app.migration.pubsub;

import io.micronaut.context.annotation.ConfigurationProperties;
import lombok.Data;

@ConfigurationProperties("gcp.pubsub.publisher.migration-converter-job-responses")
@Data
public class PubSubResponseConfig {

    String topic;
}
