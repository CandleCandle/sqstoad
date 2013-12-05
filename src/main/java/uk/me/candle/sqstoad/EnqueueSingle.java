package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnqueueSingle extends AbstractCliAction {
	private static final Logger LOG = LoggerFactory.getLogger(EnqueueSingle.class);

	@Argument(index = 0, metaVar = "queueName", required = true, hidden = false, usage = "The name of the queue to enqueue the item")
	private String queueToDownload;

	@Argument(index = 1, metaVar = "filename", required = true, hidden = false, usage = "Filename to read the message from - the contents of the file will be enqueued verbatim.")
	private String filenameToRead;

	public EnqueueSingle(ClientConfiguration clientConfiguration) {
		super(clientConfiguration);
	}

	public Void call() throws Exception {
		enqueueItem(getSqsClient(), queueToDownload, filenameToRead);
		return null;
	}

	public static void enqueueItem(AmazonSQS sqsClient, String queueName, String filename) throws Exception {
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(filename);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[4096];
			int len = 0;
			while ((len = fis.read(buffer)) > 0) {
				baos.write(buffer, 0, len);
			}
			String message = new String(baos.toString("UTF-8"));
			SendMessageRequest request = new SendMessageRequest(queueName, message);
			SendMessageResult result = sqsClient.sendMessage(request);
			LOG.debug("sent {} with new message id: {}", filename, result.getMessageId());
		} finally {
			try {
				if (fis != null) fis.close();
			} catch (IOException ex) {
				LOG.error(ex.getMessage(), ex);
			}
		}
	}

}
