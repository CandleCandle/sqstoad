package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
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

public class DownloadQueue extends AbstractCliAction {

    @Argument(index = 0, metaVar = "queueName", hidden = false, usage = "The name of the queue to download")
    private String queueToDownload;

    @Argument(index = 1, metaVar = "filename", hidden = false, usage = "Filename to create and write the messages into.")
    private String filenameToCreate;

    public DownloadQueue(ClientConfiguration clientConfiguration) {
        super(clientConfiguration);
    }

    public Void call() throws Exception {
        downloadQueue(getSqsClient(), queueToDownload, filenameToCreate);
        return null;
    }

	static void downloadQueue(AmazonSQS client, String queueName, String filename) throws IOException {
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

}
