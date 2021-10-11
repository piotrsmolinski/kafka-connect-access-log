package dev.psmolinski.kafka.connect;

import org.apache.kafka.common.Configurable;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.eclipse.jetty.server.CustomRequestLog;
import org.eclipse.jetty.server.Slf4jRequestLogWriter;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Workaround for log handler installation:
 * <a href="https://github.com/apache/kafka/blob/3.0.0/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/rest/RestServer.java#L278-L282">github</a>
 * <br/>
 * The problems in the original code:
 * <ul>
 * <li>handler uses fixed category shared with other parts of the RestServer</li>
 * <li>the handler is injected into place where it is never invoked</li>
 * </ul>
 * For the installation add the following entries in the connect-distributed.properties:
 * <pre>
 * # activation:
 * rest.servlet.initializor.classes = dev.psmolinski.kafka.connect.InstallRequestLog
 * # optional, the default value is compatible with the original one
 * request.logger.name = org.apache.kafka.connect.runtime.rest.RestServer.request
 * </pre>
 */
public class InstallRequestLog implements Consumer<ServletContextHandler>, Configurable {

    private String loggerName;

    @Override
    public void configure(Map<String, ?> map) {
        Config config = new Config(map);
        this.loggerName = config.loggerName();
    }

    @Override
    public void accept(final ServletContextHandler context) {
        Slf4jRequestLogWriter slf4jRequestLogWriter = new Slf4jRequestLogWriter();
        slf4jRequestLogWriter.setLoggerName(loggerName);
        CustomRequestLog requestLog = new CustomRequestLog(slf4jRequestLogWriter, CustomRequestLog.NCSA_FORMAT+" %{ms}T");
        RequestLogHandler requestLogHandler = new RequestLogHandler();
        requestLogHandler.setRequestLog(requestLog);
        context.insertHandler(requestLogHandler);
        // this won't work
        // context.getServer().setRequestLog(requestLog);
    }

    private static class Config extends AbstractConfig {

        public Config(Map<?, ?> originals) {
            super(configDef, originals, Collections.emptyMap(), true);
        }

        public String loggerName() {
            return getString("request.logger.name");
        }

        private static ConfigDef configDef = new ConfigDef()
          .define(
            "request.logger.name",
            ConfigDef.Type.STRING,
            "org.apache.kafka.connect.runtime.rest.RestServer.request",
            ConfigDef.Importance.LOW,
            ""
          );
    }

}
