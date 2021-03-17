package no.ssb.rawdata.converter.app.migration.pubsub;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PubSubAdmin {

    public static void createSubscriptionIfNotExists(
            SubscriptionAdminClient subscriptionAdminClient,
            String projectId,
            String topic,
            String subscriptionName,
            int ackDeadlineSeconds) {
        createSubscriptionIfNotExists(subscriptionAdminClient, projectId, topic, subscriptionName, ackDeadlineSeconds, null, null, null);
    }

    public static void createSubscriptionIfNotExists(
            SubscriptionAdminClient subscriptionAdminClient,
            String projectId,
            String topic,
            String subscriptionName,
            int ackDeadlineSeconds,
            Integer maxRedeliveryAttemptsBeforeSendingToDlq,
            String dlqProjectId,
            String dlqTopic) {

        ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topic);
        ProjectSubscriptionName projectSubscriptionName = ProjectSubscriptionName.of(projectId, subscriptionName);

        Subscription.Builder builder = Subscription.newBuilder();
        builder.setName(projectSubscriptionName.toString())
                .setTopic(projectTopicName.toString())
                .setPushConfig(PushConfig.getDefaultInstance())
                .setAckDeadlineSeconds(ackDeadlineSeconds);

        if (dlqTopic != null) {
            ProjectTopicName dlqProjectTopicName = ProjectTopicName.of(dlqProjectId, dlqTopic);
            builder.setDeadLetterPolicy(DeadLetterPolicy.newBuilder()
                    .setDeadLetterTopic(dlqProjectTopicName.toString())
                    .setMaxDeliveryAttempts(maxRedeliveryAttemptsBeforeSendingToDlq)
                    .build());
        }
        Subscription subscription = builder.build();

        ProjectName projectName = ProjectName.of(projectId);
        if (subscriptionExists(subscriptionAdminClient, projectName, projectSubscriptionName, 25)) {
            return;
        }

        log.info("Creating subscription: {}", projectSubscriptionName.toString());
        subscriptionAdminClient.createSubscription(subscription);
    }

    public static void createTopicIfNotExists(TopicAdminClient topicAdminClient, String projectId, String topic) {
        ProjectTopicName projectTopicName = ProjectTopicName.of(projectId, topic);
        if (!topicExists(topicAdminClient, ProjectName.of(projectId), projectTopicName, 25)) {
            log.info("Creating topic: {}", projectTopicName.toString());
            topicAdminClient.createTopic(projectTopicName);
        }
    }

    public static boolean topicExists(TopicAdminClient topicAdminClient, ProjectName projectName, ProjectTopicName projectTopicName, int pageSize) {
        log.trace("Listing topics on project: {}", projectName.getProject());
        TopicAdminClient.ListTopicsPagedResponse listResponse = topicAdminClient
                .listTopics(ListTopicsRequest.newBuilder()
                        .setProject(projectName.toString())
                        .setPageSize(pageSize)
                        .build());
        for (Topic topic : listResponse.iterateAll()) {
            if (topic.getName().equals(projectTopicName.toString())) {
                return true;
            }
        }
        while (listResponse.getPage().hasNextPage()) {
            listResponse = topicAdminClient
                    .listTopics(ListTopicsRequest.newBuilder()
                            .setProject(projectName.toString())
                            .setPageToken(listResponse.getNextPageToken())
                            .setPageSize(pageSize)
                            .build());
            for (Topic topic : listResponse.iterateAll()) {
                if (topic.getName().equals(projectTopicName.toString())) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean subscriptionExists(SubscriptionAdminClient subscriptionAdminClient, ProjectName projectName, ProjectSubscriptionName projectSubscriptionName, int pageSize) {
        log.trace("Listing subscriptions on project: {}", projectName.getProject());
        SubscriptionAdminClient.ListSubscriptionsPagedResponse listResponse = subscriptionAdminClient
                .listSubscriptions(ListSubscriptionsRequest.newBuilder()
                        .setProject(projectName.toString())
                        .setPageSize(pageSize)
                        .build());
        for (Subscription subscription : listResponse.iterateAll()) {
            if (subscription.getName().equals(projectSubscriptionName.toString())) {
                return true;
            }
        }
        while (listResponse.getPage().hasNextPage()) {
            listResponse = subscriptionAdminClient
                    .listSubscriptions(ListSubscriptionsRequest.newBuilder()
                            .setProject(projectName.toString())
                            .setPageToken(listResponse.getNextPageToken())
                            .setPageSize(pageSize)
                            .build());
            for (Subscription subscription : listResponse.iterateAll()) {
                if (subscription.getName().equals(projectSubscriptionName.toString())) {
                    return true;
                }
            }
        }
        return false;
    }
}
