package io.kestra.plugin.singer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.tasks.scripts.AbstractLogThread;
import io.kestra.core.tasks.scripts.AbstractPython;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.singer.models.Metric;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPythonSinger extends AbstractPython {
    protected static final ObjectMapper MAPPER = JacksonMapper.ofJson()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    protected static final TypeReference<Map<String, String>> TYPE_REFERENCE = new TypeReference<>() {};

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient List<Metric> metrics = new ArrayList<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> stateRecords = new HashMap<>();

    @Schema(
        title = "The name of singer state file"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @Builder.Default
    protected String stateName = "singer-state";

    @Schema(
        title = "Override default pip packages to use a specific version"
    )
    @PluginProperty(dynamic = true)
    protected List<String> pipPackages;

    @Schema(
        title = "Override default singer command"
    )
    @PluginProperty(dynamic = true)
    protected String command;

    abstract public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException, IOException;

    abstract public List<String> pipPackages();

    abstract protected String command();

    protected String finalCommand(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.command != null ? runContext.render(this.command) : this.command();
    }

    protected void initVirtualEnv(RunContext runContext, Logger logger) throws Exception {
        this.setupVirtualEnv(logger, runContext, this.pipPackages != null ? runContext.render(this.pipPackages) : this.pipPackages());
        this.writeLoggingConf(logger, runContext);
        this.writeSingerFiles("config.json", this.configuration(runContext));
    }

    protected void writeSingerFiles(String filename, String content) throws IOException {
        FileUtils.writeStringToFile(
            new File(workingDirectory.toFile(), filename),
            content,
            StandardCharsets.UTF_8
        );
    }

    protected void writeSingerFiles(String filename, Object map) throws IOException {
        this.writeSingerFiles(filename, MAPPER.writeValueAsString(map));
    }

    protected void writeLoggingConf(Logger logger, RunContext runContext) throws Exception {
        String template = IOUtils.toString(
            Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("singer/logging.conf")),
            StandardCharsets.UTF_8
        );

        this.writeSingerFiles("logging.conf", template);

        this.run(
            runContext,
            logger,
            workingDirectory,
            this.finalCommandsWithInterpreter(
                "find lib/  -type f -name logging.conf | grep \"/singer/\" | xargs cp logging.conf"
            ),
            this.environnementVariable(runContext),
            this.logThreadSupplier(logger, null)
        );
    }

    protected void setupVirtualEnv(Logger logger, RunContext runContext, List<String> requirements) throws Exception {
        ArrayList<String> finalRequirements = new ArrayList<>(requirements);
        finalRequirements.add("python-json-logger");

        this.run(
            runContext,
            logger,
            workingDirectory,
            this.finalCommandsWithInterpreter(this.virtualEnvCommand(runContext, finalRequirements)),
            this.environnementVariable(runContext),
            this.logThreadSupplier(logger, null)
        );
    }

    protected File writeJsonTempFile(Object value) throws IOException {
        File tempFile = workingDirectory.resolve(IdUtils.create() + ".json").toFile();

        try (FileWriter fileWriter = new FileWriter(tempFile)) {
            fileWriter.write(MAPPER.writeValueAsString(value));
            fileWriter.flush();
        }

        return tempFile;
    }

    public URI saveState(RunContext runContext) throws IOException {
        return this.saveState(runContext, this.stateName, this.stateRecords);
    }

    public URI saveState(RunContext runContext, String state, Map<String, Object> stateRecords) throws IOException {
        File tempFile = this.writeJsonTempFile(stateRecords);
        return runContext.putTaskStateFile(tempFile, state, "state.json");
    }

    protected Map<String, String> environnementVariable(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        HashMap<String, String> env = new HashMap<>(this.finalEnv());
        env.put("LOGGING_CONF_FILE", "logging.conf");

        return env;
    }

    protected void saveSingerMetrics(RunContext runContext) {
        this.metrics
            .forEach(metric -> {
                synchronized (this) {
                    String name = "singer." + metric.getMetric().replaceAll("[_-]", ".");
                    String[] tags = metric
                        .getTags()
                        .entrySet()
                        .stream()
                        .filter(e -> e.getValue() instanceof String)
                        .flatMap(e -> Stream.of(e.getKey().toLowerCase(Locale.ROOT), ((String) e.getValue()).toLowerCase(Locale.ROOT)))
                        .toArray(String[]::new);

                    switch (metric.getType()) {
                        case counter:
                            runContext.metric(Counter.of(name, metric.getValue(), tags));
                            break;
                        case timer:
                            runContext.metric(Timer.of(name, Duration.ofNanos(Double.valueOf(metric.getValue() * 1e+9).longValue()), tags));
                            break;
                    }
                }
            });
    }

    public void stateMessage(Map<String, Object> stateValue) {
        this.stateRecords.putAll(stateValue);
    }

    protected LogSupplier logThreadSupplier(Logger logger, Consumer<String> consumer) {
        return (inputStream, isStdErr) -> {
            AbstractLogThread thread;
            if (isStdErr || consumer == null) {
                thread = new singerLogParser(inputStream, logger, this.metrics);
                thread.setName("singer-log-err");
            } else {
                thread = new SingerLogSync(inputStream, consumer);
                thread.setName("singer-log-out");
            }

            thread.start();

            return thread;
        };
    }

    public static class SingerLogSync extends AbstractLogThread {
        private final Consumer<String> consumer;

        public SingerLogSync(InputStream inputStream, Consumer<String> consumer) {
            super(inputStream);
            this.consumer = consumer;
        }

        @Override
        protected void call(String line) {
            this.consumer.accept(line);
        }
    }

    protected static class singerLogParser extends AbstractLogThread {
        private final Logger logger;
        private final List<Metric> metrics;

        public singerLogParser(InputStream inputStream, Logger logger, List<Metric> metrics) {
            super(inputStream);
            this.logger = logger;
            this.metrics = metrics;
        }

        @Override
        protected void call(String line) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, String> jsonLog = (Map<String, String>) MAPPER.readValue(line, Object.class);

                if (jsonLog.containsKey("message") && jsonLog.get("message") != null && jsonLog.get("message").startsWith("METRIC: {")) {
                    metrics.add(MAPPER.readValue(jsonLog.get("message").substring(8), Metric.class));
                    return;
                }

                HashMap<String, String> additional = new HashMap<>(jsonLog);
                additional.remove("asctime");
                additional.remove("name");
                additional.remove("message");
                additional.remove("levelname");

                String format = "[Date: {}] [Name: {}] {}{}";
                String[] args = new String[]{
                    jsonLog.get("asctime"),
                    jsonLog.get("name"),
                    jsonLog.get("message") != null ? jsonLog.get("message") + " " : "",
                    additional.size() > 0 ? additional.toString() : ""
                };

                switch (jsonLog.get("levelname")) {
                    case "DEBUG":
                        logger.debug(format, (Object[]) args);
                        break;
                    case "INFO":
                        logger.info(format, (Object[]) args);
                        break;
                    case "WARNING":
                        logger.warn(format, (Object[]) args);
                        break;
                    default:
                        logger.error(format, (Object[]) args);
                }
            } catch (JsonProcessingException e) {
                logger.info(line.trim());
            }
        }
    }

}
