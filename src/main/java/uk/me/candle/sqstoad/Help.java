package uk.me.candle.sqstoad;

import java.io.ByteArrayOutputStream;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Help implements CliAction {
    private static final Logger LOG = LoggerFactory.getLogger(Help.class);

    @Option(name = "--all", aliases = {"-a"}, usage = "if present, show detail for all commands")
    private boolean showAll = false;

    @Argument(index = 0, metaVar = "helpItem", required = false, hidden = false, usage = "action on which to get detailed help")
    private String actionName;

    public Void call() throws Exception {
        StringBuilder builder = new StringBuilder("usage: sqstoad <action> <action arguments>\n\nWhere <action> can be one of:\n");

        LOG.debug("showAll: {}", showAll);
        LOG.debug("actionName: {}", actionName);

        for (CliAction.Actions action : CliAction.Actions.values()) {
            builder.append("\t").append(action.getCommand()).append("\n");
        }

        builder.append("Detailed usage:\n");
        for (CliAction.Actions action : CliAction.Actions.values()) {
            LOG.debug("found action: {}", action);
            if (showAll || CliAction.Actions.HELP.getCommand().equals(action.getCommand()) || action.getCommand().equals(actionName)) {
                builder.append("\t").append(action.getCommand());
                CmdLineParser parser = new CmdLineParser(action.newInstance(null));

                ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
                parser.printSingleLineUsage(baos1);
                builder.append(new String(baos1.toByteArray())).append("\n");

                ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
                parser.printUsage(baos2);
                builder.append(new String(baos2.toByteArray())).append("\n");
            }
        }

        System.out.println(builder.toString());

        return null;
    }

}
