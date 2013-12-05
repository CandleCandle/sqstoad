package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import java.io.IOException;
import org.kohsuke.args4j.Argument;

public class DrainQueue extends AbstractCliAction {

	@Argument(index = 0, metaVar = "queueName", required = true, hidden = false, usage = "The name of the queue to drain")
	private String queueToDrain;

	public DrainQueue(ClientConfiguration clientConfiguration) {
		super(clientConfiguration);
	}

	public Void call() throws Exception {
		drainQueue(getSqsClient(), queueToDrain);
		return null;
	}

	private static void drainQueue(AmazonSQS client, String queueName) throws IOException {
		DownloadQueue.downloadQueue(client, queueName, null);
	}

}
