package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	private static final String EU_WEST_1_ENDPOINT = "eu-west-1.queue.amazonaws.com";

	public static void main(String[] args) {

		ClientConfiguration conf = createClientConfiguration();

		AmazonSQSClient client = new AmazonSQSClient(conf);
		client.setEndpoint(EU_WEST_1_ENDPOINT);

		AmazonSNSClient sns = new AmazonSNSClient(conf);
		sns.setEndpoint(EU_WEST_1_ENDPOINT);

		String action = args.length > 0 ? args[0] : "--help";

		if("list-queues".equals(action)) listQueues(client);
		else if("list-topics".equals(action)) listTopics(sns);
//		else if ("drain-queue".equals(action)) drainQueue(client, args[1]);
		else showHelp();
	}

	private static void showHelp() {
		System.out.println("Usage: java -jar sqsToad.jar <action>");
		System.out.println("");
		System.out.println("Actions:");
		System.out.println("    list-queues");
		System.out.println("    list-topics");
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
