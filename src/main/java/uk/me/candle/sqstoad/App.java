package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import java.util.Arrays;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
	private static final Logger LOG = LoggerFactory.getLogger(App.class);
	static final String EU_WEST_1_SQS_ENDPOINT = "sqs.eu-west-1.amazonaws.com";
	static final String EU_WEST_1_SNS_ENDPOINT = "sns.eu-west-1.amazonaws.com";

	public static void main(String[] args) throws Exception {

		ClientConfiguration conf = createClientConfiguration();

		String command = args.length > 0 ? args[0] : "help";

        for (CliAction.Actions action : CliAction.Actions.values()) {
            if (action.getCommand().equals(command)) {
                CliAction cmd = action.newInstance(conf);
                CmdLineParser parser = new CmdLineParser(cmd);
                parser.parseArgument(Arrays.copyOfRange(args, 1, args.length));

                cmd.call();
            }
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
