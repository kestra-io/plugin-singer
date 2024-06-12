package io.kestra.plugin.singer;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.runners.AbstractLogConsumer;
import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.models.tasks.runners.ScriptService;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.RunnerType;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.singer.models.Metric;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;

import jakarta.validation.constraints.NotNull;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPythonSinger extends Task {
    protected static final ObjectMapper MAPPER = JacksonMapper.ofJson()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    protected static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<>() {
    };
    private static final String DEFAULT_IMAGE = "python:3.10.12";

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient List<Metric> metrics = new ArrayList<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Object> stateRecords = new HashMap<>();

    @Getter(AccessLevel.NONE)
    protected transient Path workingDirectory;

    @Schema(
        title = "The name of singer state file"
    )
    @PluginProperty(dynamic = true)
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

    @Schema(
        title = "Docker options when for the `DOCKER` runner",
        defaultValue = "{image=" + DEFAULT_IMAGE + ", pullPolicy=ALWAYS}"
    )
    @PluginProperty
    @Builder.Default
    protected DockerOptions docker = DockerOptions.builder().build();

    protected DockerOptions injectDefaults(DockerOptions original) {
        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }

        return builder.build();
    }

    abstract public Map<String, Object> configuration(RunContext runContext) throws IllegalVariableEvaluationException, IOException;

    abstract public List<String> pipPackages();

    abstract protected String command();

    protected String finalCommand(RunContext runContext) throws IllegalVariableEvaluationException {
        return this.command != null ? runContext.render(this.command) : this.command();
    }

    protected void setup(RunContext runContext) throws Exception {
        CommandsWrapper commandsWrapper = new CommandsWrapper(runContext);
        workingDirectory = commandsWrapper.getWorkingDirectory();

        configSetupCommands(runContext);

        commandsWrapper.withWarningOnStdErr(true)
            .withRunnerType(RunnerType.DOCKER)
            .withDockerOptions(this.injectDefaults(getDocker()))
            .withLogConsumer(new DefaultLogConsumer(runContext))
            .withCommands(ScriptService.scriptCommands(
                List.of("/bin/sh", "-c"),
                Stream.of(
                    pipInstallCommands(runContext),
                    logSetupCommands()
                ).flatMap(Function.identity()).toList(),
                Collections.emptyList()
            )).run();
    }

    protected void run(RunContext runContext, String command, AbstractLogConsumer logConsumer) throws Exception {
        new CommandsWrapper(runContext)
            .withWarningOnStdErr(true)
            .withRunnerType(RunnerType.DOCKER)
            .withDockerOptions(this.injectDefaults(getDocker()))
            .withLogConsumer(logConsumer)
            .withCommands(ScriptService.scriptCommands(
                List.of("/bin/sh", "-c"),
                Collections.emptyList(),
                List.of(command)
            ))
            .withEnv(this.environmentVariables(runContext))
            .run();
    }

    protected Stream<String> pipInstallCommands(RunContext runContext) throws Exception {
        ArrayList<String> finalRequirements = new ArrayList<>(this.pipPackages != null ? runContext.render(this.pipPackages) : this.pipPackages());
        finalRequirements.add("python-json-logger");

        return Stream.concat(
            Stream.of(
                "set -o errexit",
                "pip install pip --upgrade > /dev/null"
            ),
            finalRequirements.stream().map("pip install --target . %s > /dev/null"::formatted)
        );
    }


    protected Stream<String> logSetupCommands() throws Exception {
        String template = IOUtils.toString(
            Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("singer/logging.conf")),
            StandardCharsets.UTF_8
        );

        this.writeSingerFiles("logging.conf", template);

        return Stream.of(
            "find .  -type f -name logging.conf | grep \"/singer/\" | xargs -r cp logging.conf"
        );
    }

    protected void configSetupCommands(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        this.writeSingerFiles("config.json", MAPPER.writeValueAsString(this.configuration(runContext)));
    }

    protected Map<String, String> environmentVariables(RunContext runContext) throws IllegalVariableEvaluationException, IOException {
        return new HashMap<>(Map.of(
            "PYTHONUNBUFFERED", "true",
            "PIP_ROOT_USER_ACTION", "ignore",
            "LOGGING_CONF_FILE", "logging.conf",
            "PYTHONPATH", "."
        ));
    }

    protected void writeSingerFiles(String filename, String content) throws IOException {
        FileUtils.writeStringToFile(
            new File(this.workingDirectory.toFile(), filename),
            content,
            StandardCharsets.UTF_8
        );
    }

    protected void writeSingerFiles(String filename, Object map) throws IOException {
        this.writeSingerFiles(filename, MAPPER.writeValueAsString(map));
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
                        .flatMap(e -> Stream.of(
                            e.getKey().toLowerCase(Locale.ROOT),
                            ((String) e.getValue()).toLowerCase(Locale.ROOT)
                        ))
                        .toArray(String[]::new);

                    switch (metric.getType()) {
                        case counter -> runContext.metric(Counter.of(name, metric.getValue(), tags));
                        case timer -> runContext.metric(Timer.of(name,
                            Duration.ofNanos(Double.valueOf(metric.getValue() * 1e+9).longValue()),
                            tags
                        ));
                    }
                }
            });
    }

    protected File writeJsonTempFile(Object value) throws IOException {
        File tempFile = workingDirectory.resolve(IdUtils.create() + ".json").toFile();

        try (FileWriter fileWriter = new FileWriter(tempFile)) {
            fileWriter.write(MAPPER.writeValueAsString(value));
            fileWriter.flush();
        }

        return tempFile;
    }

    public URI saveState(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        return this.saveState(runContext, runContext.render(this.stateName), this.stateRecords);
    }

    public URI saveState(RunContext runContext, String state, Map<String, Object> stateRecords) throws IOException {
        File tempFile = this.writeJsonTempFile(stateRecords);
        return runContext.storage().putTaskStateFile(tempFile, state, "state.json");
    }

    public void stateMessage(Map<String, Object> stateValue) {
        this.stateRecords.putAll(stateValue);
    }

    public static class SingerLogDispatcher extends AbstractLogConsumer {
        private final SingerLogParser singerLogParser;
        private SingerLogSync singerLogSync;

        public SingerLogDispatcher(RunContext runContext, List<Metric> metrics, Consumer<String> consumer) {
            singerLogParser = new SingerLogParser(runContext.logger(), metrics);
            if (consumer != null) {
                singerLogSync = new SingerLogSync(consumer);
            }
        }

        @Override
        public void accept(String line, Boolean isStdErr) {
            if (isStdErr) {
                singerLogParser.accept(line, isStdErr);
                return;
            }

            if (singerLogSync != null) {
                singerLogSync.accept(line, isStdErr);
            }
        }
    }

    private static class SingerLogSync extends AbstractLogConsumer {
        private final Consumer<String> consumer;

        public SingerLogSync(Consumer<String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void accept(String log, Boolean isStdErr) {
            this.consumer.accept(log);
        }
    }

    private static class SingerLogParser extends AbstractLogConsumer {
        private final Logger logger;
        private final List<Metric> metrics;

        public SingerLogParser(Logger logger, List<Metric> metrics) {
            this.logger = logger;
            this.metrics = metrics;
        }

        @Override
        public void accept(String line, Boolean isStdErr) {
            try {
                @SuppressWarnings("unchecked")
                Map<String, String> jsonLog = (Map<String, String>) MAPPER.readValue(line, Object.class);

                if (jsonLog.containsKey("message") && jsonLog.get("message") != null && jsonLog.get("message")
                    .startsWith("METRIC: {")) {
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
                    !additional.isEmpty() ? additional.toString() : ""
                };

                switch (jsonLog.get("levelname")) {
                    case "DEBUG" -> logger.debug(format, (Object[]) args);
                    case "INFO" -> logger.info(format, (Object[]) args);
                    case "WARNING" -> logger.warn(format, (Object[]) args);
                    default -> logger.error(format, (Object[]) args);
                }
            } catch (JsonProcessingException e) {
                logger.info(line.trim());
            }
        }
    }

}
