package uk.me.candle.sqstoad;

import com.amazonaws.ClientConfiguration;
import com.google.common.collect.Lists;
import java.net.URI;
import java.util.Arrays;
import org.kohsuke.args4j.CmdLineException;
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

		boolean found = false;
		for (CliAction.Actions action : CliAction.Actions.values()) {
			if (action.getCommand().equals(command)) {

				runCommand(action, conf, args);
				found = true;
			}
		}
		if (!found) {
			runCommand(CliAction.Actions.HELP, conf, args);
		}
	}

	private static ClientConfiguration createClientConfiguration() {
		ClientConfiguration cc = new ClientConfiguration();
		String proxyProp = System.getenv("https_proxy");
		if (proxyProp != null) {
			LOG.debug("Found an HTTPS proxy {}", proxyProp);
			if (proxyProp.startsWith("http")) {
				setProxyFromUri(proxyProp, cc);
			} else {
				setProxyFromBasic(proxyProp, cc);
			}

		}
		return cc;
	}

	private static void setProxyFromBasic(String proxyProp, ClientConfiguration cc) {
		String[] parts = proxyProp.split(":");
		cc.setProxyHost(parts[0]);
		cc.setProxyPort(Integer.parseInt(parts[1]));
	}

	private static void setProxyFromUri(String proxyProp, ClientConfiguration cc) {
		URI uri = URI.create(proxyProp);
		cc.setProxyHost(uri.getHost());
		cc.setProxyPort(uri.getPort());
	}

	private static void runCommand(CliAction.Actions action, ClientConfiguration conf, String[] args) throws Exception, CmdLineException {
		CliAction cmd = action.newInstance(conf);
		CmdLineParser parser = new CmdLineParser(cmd);
		LOG.debug("action: {}", cmd.getClass().getName());
		LOG.debug("args: {}", Lists.newArrayList(Arrays.copyOfRange(args, 1, args.length)));
		parser.parseArgument(Arrays.copyOfRange(args, 1, args.length));

		cmd.call();
	}

}
