package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.ListTopicsRequest;
import com.amazonaws.services.sns.model.ListTopicsResult;
import com.amazonaws.services.sns.model.Topic;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	private static final String EU_WEST_1_SQS_ENDPOINT = "sqs.eu-west-1.amazonaws.com";
	private static final String EU_WEST_1_SNS_ENDPOINT = "sns.eu-west-1.amazonaws.com";

	public static void main(String[] args) throws Exception {

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
		else if("download-queue".equals(action)) {
            if (args.length > 2) {
                downloadQueue(client, args[1], args[2]);
			} else {
				showHelp();
			}
        } else if("drain-queue".equals(action)) {
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
		System.out.println("    download-queue <queue-name> <file-name>");
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


	private static void drainQueue(AmazonSQSClient client, String queueName) throws IOException {
        downloadQueue(client, queueName, null);
	}

	private static void downloadQueue(AmazonSQSClient client, String queueName, String filename) throws IOException {
        ZipOutputStream zos = null;
        long startTime = System.currentTimeMillis();
        int count = 0;
        try {
            if (filename != null) {
                zos = new ZipOutputStream(new FileOutputStream(filename));
            }
            while (true) {
                ReceiveMessageResult r = client.receiveMessage(new ReceiveMessageRequest()
                        .withMaxNumberOfMessages(10)
                        .withQueueUrl(queueName));
                List<DeleteMessageBatchRequestEntry> d = new ArrayList<DeleteMessageBatchRequestEntry>();
                for (Message m : r.getMessages()) {
                    if (zos != null) {
                        ZipEntry zipBody = new ZipEntry(queueName + "/" + m.getMessageId() + "/body");
                        zos.putNextEntry(zipBody);
                        zos.write(m.getBody().getBytes(Charset.defaultCharset()));
                        zos.closeEntry();

                        ZipEntry zipAttrs = new ZipEntry(queueName + "/" + m.getMessageId() + "/attributes");
                        zos.putNextEntry(zipAttrs);
                        zos.write(m.getAttributes().toString().getBytes(Charset.defaultCharset()));
                        zos.closeEntry();
                    }
                    d.add(new DeleteMessageBatchRequestEntry()
                            .withId(m.getMessageId())
                            .withReceiptHandle(m.getReceiptHandle()));
                }
                if (d.size() > 0) {
                    client.deleteMessageBatch(new DeleteMessageBatchRequest()
                            .withQueueUrl(queueName)
                            .withEntries(d));
                    count += d.size();
                } else {
                    break;
                }
            }
        } finally {
            if (zos != null) zos.close();
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
