package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListQueues extends AbstractCliAction {
    private static final Logger LOG = LoggerFactory.getLogger(ListQueues.class);

    public ListQueues(ClientConfiguration clientConfiguration) {
        super(clientConfiguration);
    }

    public Void call() throws Exception {
        listQueues(getSqsClient());
        return null;
    }

	private static void listQueues(AmazonSQS client) {
		LOG.debug("Listing Queues");
		ListQueuesResult listQueues = client.listQueues();
		for (String s : listQueues.getQueueUrls()) {
			System.out.println(s);
		}
	}

}
