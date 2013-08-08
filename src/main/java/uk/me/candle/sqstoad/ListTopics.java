package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListTopics extends AbstractCliAction {
    private static final Logger LOG = LoggerFactory.getLogger(ListTopics.class);

    public ListTopics(ClientConfiguration clientConfiguration) {
        super(clientConfiguration);
    }

    public Void call() throws Exception {
        listTopics(getSnsClient());
        return null;
    }

	private static void listTopics(AmazonSNS client) {
		LOG.debug("Listing Topics");
        ListTopicsRequest request = new ListTopicsRequest();
        ListTopicsResult list;
        do {
            list = client.listTopics(request);
            for (Topic t : list.getTopics()) {
                System.out.println(t.getTopicArn());
            }
            request = new ListTopicsRequest(list.getNextToken());
        } while (list.getNextToken() != null);
	}

}
