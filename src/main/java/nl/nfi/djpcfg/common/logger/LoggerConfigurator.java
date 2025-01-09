package nl.nfi.djpcfg.common.logger;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.Configurator;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.tyler.TylerConfiguratorBase;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeAndTimeBasedRollingPolicy;
import ch.qos.logback.core.util.FileSize;

import static nl.nfi.djpcfg.common.HostUtils.hostname;

public final class LoggerConfigurator extends TylerConfiguratorBase implements Configurator {

    static {
        System.setProperty("slf4j.internal.verbosity", "ERROR");
    }

    private static final String LOG_PATTERN = "%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{2000} -%kvp- %msg%n";
    private static final String LOG_DIRECTORY_PATH = System.getProperty("LOG_DIRECTORY_PATH");
    private static final String LOG_FILE_PATH = LOG_DIRECTORY_PATH + "/" + hostname() + ".log";
    private static final String LOG_ROLL_PATH_PATTERN = LOG_DIRECTORY_PATH + "/" + hostname() + ".%d{yyyy-MM-dd}.%i.gz";

    @Override
    public ExecutionStatus configure(final LoggerContext loggerContext) {
        if (LOG_DIRECTORY_PATH == null) {
            return ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY;
        }

        setContext(loggerContext);
        // suppress Netty logging
        setupLogger("io.grpc.netty", "WARN", null);

        final Appender<ILoggingEvent> appender = createFileAppender();
        final Logger root = setupLogger("ROOT", "DEBUG", null);
        root.addAppender(appender);

        return ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY;
    }

    private Appender<ILoggingEvent> createFileAppender() {
        final RollingFileAppender<ILoggingEvent> appender = new RollingFileAppender<>();
        appender.setContext(context);
        appender.setName("FILE");
        appender.setFile(LOG_FILE_PATH);
        // appender.setPrudent(true);

        final SizeAndTimeBasedRollingPolicy<ILoggingEvent> rollingPolicy = new SizeAndTimeBasedRollingPolicy<>();
        rollingPolicy.setContext(context);
        rollingPolicy.setFileNamePattern(LOG_ROLL_PATH_PATTERN);
        rollingPolicy.setMaxFileSize(FileSize.valueOf("1GB"));
        rollingPolicy.setMaxHistory(30);
        rollingPolicy.setTotalSizeCap(FileSize.valueOf("1GB"));
        rollingPolicy.setParent(appender);
        rollingPolicy.start();

        appender.setRollingPolicy(rollingPolicy);

        final PatternLayoutEncoder layoutEncoder = new PatternLayoutEncoder();
        layoutEncoder.setContext(context);
        layoutEncoder.setPattern(LOG_PATTERN);
        layoutEncoder.setParent(appender);
        layoutEncoder.start();

        appender.setEncoder(layoutEncoder);

        appender.start();
        return appender;
    }
}
