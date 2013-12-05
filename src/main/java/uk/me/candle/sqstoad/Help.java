package uk.me.candle.sqstoad;

import java.io.ByteArrayOutputStream;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Help implements CliAction {
	private static final Logger LOG = LoggerFactory.getLogger(Help.class);
	private static final String GENERAL_USAGE = "usage: sqstoad action [action arguments]\n\n";
	private static final String LIST_ACTIONS = "Where <action> can be one of:\n\n";
	private static final String ACTION_DETAIL = "With <action>:\n\n";

	@Option(name = "--all", aliases = {"-a"}, usage = "if present, show detail for all commands")
	private boolean showAll = false;

	@Argument(index = 0, metaVar = "actionName", required = false, hidden = false, usage = "action on which to get detailed help")
	private String actionName;

	public Void call() throws Exception {
		StringBuilder builder = new StringBuilder(GENERAL_USAGE);

		LOG.debug("showAll: {}", showAll);
		LOG.debug("actionName: {}", actionName);

		if (actionName == null && !showAll) {
			builder.append(LIST_ACTIONS);
			for (CliAction.Actions action : CliAction.Actions.values()) {
				CmdLineParser parser = new CmdLineParser(action.newInstance(null));

				ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
				parser.printSingleLineUsage(baos1);

				builder.append("\t").append(action.getCommand());
				builder.append(new String(baos1.toByteArray())).append("\n");
			}
		} else {
			builder.append(ACTION_DETAIL);
			for (CliAction.Actions action : CliAction.Actions.values()) {
				LOG.debug("found action: {}", action);
				if (showAll || action.getCommand().equals(actionName)) {
					builder.append("\t").append(action.getCommand());
					CmdLineParser parser = new CmdLineParser(action.newInstance(null));

					ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
					parser.printSingleLineUsage(baos1);
					builder.append(new String(baos1.toByteArray())).append("\n");

					ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
					parser.printUsage(baos2);
					builder.append("\t\t").append(new String(baos2.toByteArray()).replace("\n", "\n\t\t")).append("\n");
				}
			}
		}


		System.out.println(builder.toString());

		return null;
	}

}
