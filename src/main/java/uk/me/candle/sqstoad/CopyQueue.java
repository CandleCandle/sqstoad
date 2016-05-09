package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
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
import org.kohsuke.args4j.Argument;

public class CopyQueue extends AbstractCliAction {

	@Argument(index = 0, metaVar = "queueName", required = true, hidden = false, usage = "The name of the queue to download")
	private String queueToDownload;

	@Argument(index = 1, metaVar = "filename", required = true, hidden = false, usage = "Filename to create and write the messages into.")
	private String filenameToCreate;
    
	@Argument(index = 2, metaVar = "timeout", required = false, hidden = false, usage = "Timeout (in seconds) to extend the visibility, set this higher for larger queues. Allow 10 seconds per 1000 messages on a good connection to AWS")
	private int timeout = 60;

	public CopyQueue(ClientConfiguration clientConfiguration) {
		super(clientConfiguration);
	}

	public Void call() throws Exception {
		copyQueue(getSqsClient(), queueToDownload, filenameToCreate, timeout);
		return null;
	}

	static void copyQueue(AmazonSQS client, String queueName, String filename, int timeout) throws IOException {
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
				List<ChangeMessageVisibilityBatchRequestEntry> entries = new ArrayList<ChangeMessageVisibilityBatchRequestEntry>();
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
					entries.add(new ChangeMessageVisibilityBatchRequestEntry()
							.withId(m.getMessageId())
                            .withVisibilityTimeout(timeout)
							.withReceiptHandle(m.getReceiptHandle()));
				}
				if (entries.size() > 0) {
					client.changeMessageVisibilityBatch(new ChangeMessageVisibilityBatchRequest()
							.withQueueUrl(queueName)
                            .withEntries(entries));
					count += entries.size();
				} else {
					break;
				}
			}
		} finally {
			if (zos != null) zos.close();
		}
		long seconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
		System.out.println("Copied " + count + " messages in " + seconds + " second" + (seconds == 1 ? "" : "s") + ".");
	}

}
