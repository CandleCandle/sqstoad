package uk.me.candle.sqstoad;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.google.common.collect.Sets;
import java.io.PrintStream;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListQueues extends AbstractCliAction {
	private static final Logger LOG = LoggerFactory.getLogger(ListQueues.class);
	private static final String ALPHA_NUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";


	public ListQueues(ClientConfiguration clientConfiguration) {
		super(clientConfiguration);
	}

	public Void call() throws Exception {
		listQueues(System.out, getSqsClient());
		return null;
	}

	static void listQueues(PrintStream out, AmazonSQS sqsClient) {
		Set<String> queues = Sets.newTreeSet();
		listQueuesWithPrefix("", queues, sqsClient);
		for (String queue : queues) {
			out.println(queue);
		}
	}

	private static void listQueuesWithPrefix(String initialPrefix, Set<String> accumumlator, AmazonSQS client) {
		ListQueuesResult queueList = listQueuesWithPrefix(initialPrefix, client);
		LOG.debug("Found {} queues with the prefix '{}'", queueList.getQueueUrls().size(), initialPrefix);
	   
		if (queueList.getQueueUrls().size() >= 1000) {
			// split it up.
			listQueuesWithSeparatePrefixes(initialPrefix, accumumlator, client);
		} else {
			accumumlator.addAll(queueList.getQueueUrls());
		}
	}

	private static void listQueuesWithSeparatePrefixes(String initialPrefix, Set<String> accumumlator, AmazonSQS client) {
		for (int i = 0; i < ALPHA_NUM.length(); ++i) {
			final String prefix = initialPrefix + ALPHA_NUM.substring(i, i+1);
			listQueuesWithPrefix(prefix, accumumlator, client);
		}
	}

	private static ListQueuesResult listQueuesWithPrefix(final String prefix, AmazonSQS client) throws AmazonClientException {
		ListQueuesRequest request = new ListQueuesRequest(prefix);
		ListQueuesResult queueList = client.listQueues(request);
		return queueList;
	}


}
