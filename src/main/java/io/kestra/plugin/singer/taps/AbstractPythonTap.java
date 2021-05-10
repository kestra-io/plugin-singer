package io.kestra.plugin.singer.taps;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.singer.AbstractPythonSinger;
import io.kestra.plugin.singer.models.DiscoverStreams;
import io.kestra.plugin.singer.models.Feature;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.models.streams.AbstractStream;
import io.kestra.plugin.singer.services.SelectedService;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPythonTap extends AbstractPythonSinger {
    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Map<String, Object>> schemas = new HashMap<>();

    @Builder.Default
    @Getter(AccessLevel.NONE)
    protected transient Map<String, Pair<File, OutputStream>> records = new HashMap<>();

    @Schema(
        title = "The list of stream configurations"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @NotEmpty
    @Valid
    protected List<StreamsConfiguration> streamsConfigurations;

    abstract public List<Feature> features();

    public void init(RunContext runContext, Logger logger) throws Exception {
        // init working dir
        Path workingDirectory = this.tmpWorkingDirectory();
        this.initVirtualEnv(runContext, logger);

        // catalog or properties
        if (this.features().contains(Feature.PROPERTIES) || this.features().contains(Feature.CATALOG)) {
            DiscoverStreams discoverProperties = this.discover(workingDirectory, runContext, logger, this.command());
            this.writeSingerFiles(workingDirectory, this.catalogName() + ".json", discoverProperties);
        }

        // state
        if (this.features().contains(Feature.STATE)) {
            try {
                InputStream taskStateFile = runContext.getTaskStateFile(this, "state.json");
                this.writeSingerFiles(workingDirectory, "state.json", IOUtils.toString(taskStateFile, StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                this.writeSingerFiles(workingDirectory, "state.json", "{}");
            }
        }
    }

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // prepare
        this.init(runContext, logger);

        // sync
        Map<AbstractStream.Type, Long> syncResult = this.runSinger(this.tapCommand(), runContext, logger);

        // metrics
        syncResult.forEach((streamType, count) -> {
            runContext.metric(Counter.of(streamType.name().toLowerCase(Locale.ROOT), count));
        });

        this.saveSingerMetrics(runContext);

        // outputs
        Output.OutputBuilder builder = Output.builder()
            .count(syncResult)
            .streams(
                this.records
                    .entrySet()
                    .stream()
                    .map(throwFunction(e -> {
                        e.getValue().getRight().flush();
                        e.getValue().getRight().close();

                        return new AbstractMap.SimpleEntry<>(
                            e.getKey(),
                            runContext.putTempFile(e.getValue().getLeft())
                        );

                    }))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            )
            .schemas(
                this.schemas
                    .entrySet()
                    .stream()
                    .map(throwFunction(e -> {
                        File tempFile = this.writeJsonTempFile(e.getValue());

                        return new AbstractMap.SimpleEntry<>(
                            e.getKey(),
                            runContext.putTempFile(tempFile)
                        );

                    }))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
            );

        if (this.features().contains(Feature.STATE) && this.state.size() > 0) {
            builder.state(this.saveState(runContext));
        }

        return builder.build();
    }

    public URI saveState(RunContext runContext) throws IOException {
        File tempFile = this.writeJsonTempFile(this.state);
        return runContext.putTaskStateFile(tempFile, this, "state.json");
    }

    protected void runSingerCommand(List<String> commands, RunContext runContext, Logger logger, FlowableEmitter<String> emitter) throws Exception {
        this.run(
            runContext,
            logger,
            workingDirectory,
            this.finalCommandsWithInterpreter(String.join(" ", commands)),
            this.environnementVariable(runContext),
            this.logThreadSupplier(logger, emitter::onNext)
        );
    }

    protected Map<AbstractStream.Type, Long> runSinger(List<String> commands, RunContext runContext, Logger logger) {
        return Flowable
            .<String>create(
                emitter -> {
                    this.runSingerCommand(commands, runContext, logger, emitter);

                    emitter.onComplete();
                },
                BackpressureStrategy.BUFFER
            )
            .map(s -> MAPPER.readValue(s, AbstractStream.class))
            .doOnNext(abstractStream -> {
                abstractStream.onNext(this);
            })
            .groupBy(AbstractStream::getType)
            .flatMapSingle(g -> g
                .count()
                .map(v -> Pair.of(g.getKey(), v))
            )
            .toMap(Pair::getLeft, Pair::getRight)
            .blockingGet();
    }

    public void schemaMessage(String stream, Map<String, Object> schema) {
        this.schemas.put(stream, schema);
    }

    public void recordMessage(String stream, Map<String, Object> record) throws IOException {
        if (!this.records.containsKey(stream)) {
            File tempFile = File.createTempFile(this.getClass().getSimpleName().toLowerCase() + "_", ".ion");
            this.records.put(stream, Pair.of(tempFile, new FileOutputStream(tempFile)));
        }

        FileSerde.write(this.records.get(stream).getRight(), record);
    }

    protected DiscoverStreams discover(Path workingDirectory, RunContext runContext, Logger logger, String command) throws Exception {
        List<String> commands = Collections.singletonList(
            "./bin/" + command + " --config config.json --discover"
        );

        StringBuilder sb = new StringBuilder();
        this.run(
            runContext,
            logger,
            workingDirectory,
            this.finalCommandsWithInterpreter(String.join(" ", commands)),
            this.environnementVariable(runContext),
            this.logThreadSupplier(logger, sb::append)
        );

        DiscoverStreams discoverStreams = MAPPER.readValue(sb.toString(), DiscoverStreams.class);

        return SelectedService.fill(discoverStreams, this.streamsConfigurations);
    }

    protected String catalogName() {
        return this.features().contains(Feature.CATALOG) ? "catalog" :
            (this.features().contains(Feature.PROPERTIES) ? "properties" : null);
    }

    public List<String> tapCommand() {
        String catalogName = this.catalogName();

        return Collections.singletonList(
            workingDirectory + "/bin/" + this.command() +
                " --config " + workingDirectory + "/" + "config.json " +
                (catalogName != null ? "--" + catalogName + " " + workingDirectory + "/" + catalogName + ".json " : "") +
                (this.features().contains(Feature.STATE) ? "--state " + workingDirectory + "/" + "state.json" : "")
        );
    }


    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Map of stream captured",
            description = "Key is stream name, value is uri of the stream file on ion kestra storage"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> streams;

        @Schema(
            title = "Map of schemas captured",
            description = "Key is stream name, value is uri of the schema file on ion kestra storage"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final Map<String, URI> schemas;

        @Schema(
            title = "Uri of the state file"
        )
        private final URI state;

        @Schema(
            title = "Counter of streams"
        )
        private final Map<AbstractStream.Type, Long> count;
    }
}