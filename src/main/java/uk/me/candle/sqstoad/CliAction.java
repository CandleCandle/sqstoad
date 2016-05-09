package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import java.util.concurrent.Callable;

public interface CliAction extends Callable<Void> {
	static enum Actions {
		HELP("help") {
			@Override public CliAction newInstance(ClientConfiguration clientConfiguration) { return new Help(); }
		},
		LIST_QUEUES("list-queues") {
			@Override public CliAction newInstance(ClientConfiguration clientConfiguration) { return new ListQueues(clientConfiguration); }
		},
		LIST_TOPICS("list-topics") {
			@Override public CliAction newInstance(ClientConfiguration clientConfiguration) { return new ListTopics(clientConfiguration); }
		},
		DRAIN_QUEUE("drain-queue") {
			@Override public CliAction newInstance(ClientConfiguration clientConfiguration) { return new DrainQueue(clientConfiguration); }
		},
		DOWNLOAD_QUEUE("download-queue") {
			@Override public CliAction newInstance(ClientConfiguration clientConfiguration) { return new DownloadQueue(clientConfiguration); }
		},
		COPY_QUEUE("copy-queue") {
			@Override public CliAction newInstance(ClientConfiguration clientConfiguration) { return new CopyQueue(clientConfiguration); }
		},
		UPLOAD_QUEUE("upload-queue") {
			@Override public CliAction newInstance(ClientConfiguration clientConfiguration) { return new UploadQueue(clientConfiguration); }
		},
		LIST_QUEUE_SUBSCRIPTIONS("list-queue-subscriptions") {
			@Override public CliAction newInstance(ClientConfiguration clientConfiguration) { return new ListQueueSubscriptions(clientConfiguration); }
		},
		ENQUEUE_SINGLE("enqueue-single") {
			@Override public CliAction newInstance(ClientConfiguration clientConfiguration) { return new EnqueueSingle(clientConfiguration); }
		}
		;

		private final String command;
		private Actions(String command) { this.command = command; }
		public String getCommand() { return command; }
		public abstract CliAction newInstance(ClientConfiguration clientConfiguration);
	}

}
