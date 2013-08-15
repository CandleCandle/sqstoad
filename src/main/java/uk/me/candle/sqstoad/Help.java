package uk.me.candle.sqstoad;

import java.io.ByteArrayOutputStream;
import org.kohsuke.args4j.CmdLineParser;

public class Help implements CliAction {

    public Void call() throws Exception {
        StringBuilder builder = new StringBuilder("usage: sqstoad <action> <action arguments>\n\nWhere <action> can be one of:\n");

        for (CliAction.Actions action : CliAction.Actions.values()) {
            builder.append("\t").append(action.getCommand()).append("\n");
        }

        builder.append("Detailed usage:\n");
        for (CliAction.Actions action : CliAction.Actions.values()) {
            builder.append("\t").append(action.getCommand()).append("\n");
            CmdLineParser parser = new CmdLineParser(action.newInstance(null));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            parser.printUsage(baos);
            builder.append(new String(baos.toByteArray())).append("\n");
        }

        System.out.println(builder.toString());

        return null;
    }

}
