package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.kohsuke.args4j.Argument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UploadQueue extends AbstractCliAction {
    private static final Logger LOG = LoggerFactory.getLogger(UploadQueue.class);

    @Argument(index = 0, metaVar = "queueName", required = true, hidden = false, usage = "The name of the queue to enqueue the items")
    private String queueToDownload;

    @Argument(index = 1, metaVar = "filename", required = true, hidden = false, usage = "Filename to read the messages from - should be in a format as produced by 'download-queue'")
    private String filenameToCreate;

    public UploadQueue(ClientConfiguration clientConfiguration) {
        super(clientConfiguration);
    }

    public Void call() throws Exception {
        uploadQueue(getSqsClient(), queueToDownload, filenameToCreate);
        return null;
    }

    private static void uploadQueue(AmazonSQS sqsClient, String queueName, String filenameToRead) throws IOException {
        ZipInputStream zis = null;

        try {
            zis = new ZipInputStream(new FileInputStream(filenameToRead));
            
            ZipEntry entry = null;
            while(null != (entry = zis.getNextEntry())) {
                System.out.println("entry: " + entry.getName());
                if (entry.getName().endsWith("/body")) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    int len = 0;
                    while ((len = zis.read(buffer)) > 0) {
                        baos.write(buffer, 0, len);
                    }
                    zis.closeEntry();

                    String message = new String(baos.toString("UTF-8"));

                    SendMessageRequest request = new SendMessageRequest(queueName, message);
                    SendMessageResult result = sqsClient.sendMessage(request);
                    LOG.debug("sent {} with new message id: {}", entry.getName(), result.getMessageId());

                }
            }

        } finally {
            if (zis != null) zis.close();
        }


    }

}
