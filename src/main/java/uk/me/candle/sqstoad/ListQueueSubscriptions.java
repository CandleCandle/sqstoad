package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.ListSubscriptionsByTopicResult;
import com.amazonaws.services.sns.model.ListSubscriptionsRequest;
import com.amazonaws.services.sns.model.ListSubscriptionsResult;
import com.amazonaws.services.sns.model.Subscription;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sqs.AmazonSQS;
import com.google.common.collect.Lists;
import java.net.URI;
import java.util.List;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListQueueSubscriptions extends AbstractCliAction {
    private static final Logger LOG = LoggerFactory.getLogger(ListQueueSubscriptions.class);

    @Argument(index = 0, metaVar = "queueName", required = true, hidden = false, usage = "The name of the queue for which to find subscriptions.")
    private String queue;


    public ListQueueSubscriptions(ClientConfiguration clientConfiguration) {
        super(clientConfiguration);
    }

    public Void call() throws Exception {
        listQueues(getSnsClient(), queue);
        return null;
    }

	private static void listQueues(AmazonSNS sns, String queue) {
        ListSubscriptionsRequest request = new ListSubscriptionsRequest();
        ListSubscriptionsResult result;
        do {
            result = sns.listSubscriptions(request);
            for (Subscription sub : result.getSubscriptions()) {
                if ("sqs".equals(sub.getProtocol())) {
                    String name = sub.getEndpoint().replaceFirst(".*:", "");
                    if (name.startsWith(queue)) {
                        System.out.println(
                                sub.getEndpoint()
                                + ","
                                + sub.getTopicArn()
                                );
                    }
                }
            }
            request = request.withNextToken(result.getNextToken());
        } while (result.getNextToken() != null);

	}

}
