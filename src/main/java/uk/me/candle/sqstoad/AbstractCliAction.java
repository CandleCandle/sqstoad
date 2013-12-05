package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;

public abstract class AbstractCliAction implements CliAction {

	ClientConfiguration clientConfiguration;

	public AbstractCliAction(ClientConfiguration clientConfiguration) {
		this.clientConfiguration = clientConfiguration;
	}

	AmazonSQS getSqsClient() {
		AmazonSQSClient client = new AmazonSQSClient(clientConfiguration);
		client.setEndpoint(App.EU_WEST_1_SQS_ENDPOINT);
		return client;
	}

	AmazonSNS getSnsClient() {
		AmazonSNSClient client = new AmazonSNSClient(clientConfiguration);
		client.setEndpoint(App.EU_WEST_1_SNS_ENDPOINT);
		return client;
	}

}
