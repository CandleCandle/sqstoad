package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	private static final String EU_WEST_1_SQS_ENDPOINT = "sqs.eu-west-1.amazonaws.com";
	private static final String EU_WEST_1_SNS_ENDPOINT = "sns.eu-west-1.amazonaws.com";

	public static void main(String[] args) {

		/*
		 *
		 * better argument parsing & error messages
		 * sqsToad -d <action> [params] to enable debug logging.
		 *
		 */

		ClientConfiguration conf = createClientConfiguration();

		AmazonSQSClient client = new AmazonSQSClient(conf);
		client.setEndpoint(EU_WEST_1_SQS_ENDPOINT);

		AmazonSNSClient sns = new AmazonSNSClient(conf);
		sns.setEndpoint(EU_WEST_1_SNS_ENDPOINT);

		String action = args.length > 0 ? args[0] : "--help";

		if("list-queues".equals(action)) listQueues(client);
		else if("list-topics".equals(action)) listTopics(sns);
		else if("drain-queue".equals(action)) {
			if (args.length > 1) {
				drainQueue(client, args[1]);
			} else {
				showHelp();
			}
		} else {
			showHelp();
		}
	}

	private static void showHelp() {
		System.out.println("Usage: java -jar sqsToad.jar <action> [action options]");
		System.out.println("");
		System.out.println("Actions:");
		System.out.println("    list-queues");
		System.out.println("    list-topics");
		System.out.println("    drain-queue <queue-name>");
		System.out.println("");
	}

	private static void listQueues(AmazonSQSClient client) {
		LOG.debug("Listing Queues");
		ListQueuesResult listQueues = client.listQueues();
		for (String s : listQueues.getQueueUrls()) {
			System.out.println(s);
		}
	}

	private static void listTopics(AmazonSNSClient client) {
		LOG.debug("Listing Topics");
		ListTopicsResult list = client.listTopics();
		for (Topic t : list.getTopics()) {
			System.out.println(t.getTopicArn());
		}
	}


	private static void drainQueue(AmazonSQSClient client, String queueName) {
		int count = 0;
		long startTime = System.currentTimeMillis();
		while (true) {
			ReceiveMessageResult r = client.receiveMessage(new ReceiveMessageRequest()
					.withMaxNumberOfMessages(10)
					.withQueueUrl(queueName));
			List<DeleteMessageBatchRequestEntry> d = new ArrayList<DeleteMessageBatchRequestEntry>();
			for (Message m : r.getMessages()) {
				d.add(new DeleteMessageBatchRequestEntry()
						.withId(m.getMessageId())
						.withReceiptHandle(m.getReceiptHandle()));
			}
			if (d.size() > 0) {
				client.deleteMessageBatch(new DeleteMessageBatchRequest()
						.withQueueUrl(queueName)
						.withEntries(d));
				count = count + d.size();
			}
			else {
				break;
			}
		}
		long seconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
		System.out.println("Drained " + count + " messages in " + seconds + " seconds.");
	}

	private static ClientConfiguration createClientConfiguration() {
		ClientConfiguration cc = new ClientConfiguration();
		String proxyProp = System.getenv("https_proxy");
		if (proxyProp != null) {
			LOG.debug("Found an HTTPS proxy {}", proxyProp);
			String[] parts = proxyProp.split(":");
			cc.setProxyHost(parts[0]);
			cc.setProxyPort(Integer.parseInt(parts[1]));
		}
		return cc;
	}

}
