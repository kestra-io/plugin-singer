package io.kestra.plugin.singer.taps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

    @Getter(AccessLevel.NONE)
    protected transient Pair<File, OutputStream> rawSingerStream;

    @Schema(
        title = "The list of stream configurations"
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @NotEmpty
    @Valid
    protected List<StreamsConfiguration> streamsConfigurations;

    @Schema(
        title = "Send singer as raw data",
        description = "Using raw data can be used with any singer target, otherwise, schemas and records will be output in kestra storage format."
    )
    @PluginProperty(dynamic = false)
    @NotNull
    @Builder.Default
    protected Boolean raw = true;

    @Getter(value = AccessLevel.NONE)
    @Builder.Default
    private transient Map<String, AtomicInteger> recordsCount =  new ConcurrentHashMap<>();

    abstract public List<Feature> features();

    public void init(RunContext runContext, Logger logger) throws Exception {
        if (this.workingDirectory == null) {
            this.workingDirectory = runContext.tempDir();
        }

        this.initVirtualEnv(runContext, logger);

        // catalog or properties
        if (this.features().contains(Feature.PROPERTIES) || this.features().contains(Feature.CATALOG)) {
            DiscoverStreams discoverProperties = this.discover(workingDirectory, runContext, logger, this.finalCommand(runContext));
            this.writeSingerFiles(this.catalogName() + ".json", discoverProperties);
        }

        // state
        if (this.features().contains(Feature.STATE)) {
            try {
                InputStream taskStateFile = runContext.getTaskStateFile(this.stateName, "state.json");
                this.writeSingerFiles("state.json", IOUtils.toString(taskStateFile, StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                this.writeSingerFiles("state.json", "{}");
            }
        }
    }

    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        // prepare
        this.init(runContext, logger);

        // sync
        Map<AbstractStream.Type, Long> syncResult = this.runSinger(this.tapCommand(runContext), runContext, logger);

        // metrics
        syncResult.forEach((streamType, count) -> {
            runContext.metric(Counter.of(streamType.name().toLowerCase(Locale.ROOT), count));
        });

        this.saveSingerMetrics(runContext);
        runContext.logger().info("Ended singer with {}", syncResult);

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
            .raw(this.raw ? runContext.putTempFile(this.rawSingerStream.getLeft()) : null)
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

        if (!this.raw && this.features().contains(Feature.STATE) && this.stateRecords.size() > 0) {
            builder.state(this.saveState(runContext));
        }

        return builder.build();
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
        Flowable<String> flowable = Flowable.create(
            emitter -> {
                this.runSingerCommand(commands, runContext, logger, emitter);

                emitter.onComplete();
            },
            BackpressureStrategy.BUFFER
        );

        Flowable<Pair<AbstractStream.Type, Long>> grouped;

        if (this.raw) {
            grouped = flowable
                .doOnNext(this::rawData)
                .groupBy(s -> AbstractStream.Type.RAW)
                .flatMapSingle(g -> g
                    .count()
                    .map(v -> Pair.of(g.getKey(), v))
                );
        } else {
            grouped = flowable
                .map(s -> MAPPER.readValue(s, AbstractStream.class))
                .doOnNext(abstractStream -> {
                    abstractStream.onNext(runContext, this);
                })
                .groupBy(AbstractStream::getType)
                .flatMapSingle(g -> g
                    .count()
                    .map(v -> Pair.of(g.getKey(), v))
                );
        }

        return grouped
            .toMap(Pair::getLeft, Pair::getRight)
            .blockingGet();
    }

    public void schemaMessage(String stream, Map<String, Object> schema) {
        this.schemas.put(stream, schema);
    }

    public void rawData(String raw) throws IOException {
        if (this.rawSingerStream == null) {
            File tempFile = File.createTempFile("message", ".json", workingDirectory.toFile());
            this.rawSingerStream =  Pair.of(tempFile, new FileOutputStream(tempFile));
        }

        this.rawSingerStream.getRight().write((raw + "\n").getBytes(StandardCharsets.UTF_8));
    }

    public void recordMessage(RunContext runContext, String stream, Map<String, Object> record) throws IOException {
        if (!this.records.containsKey(stream)) {
            File tempFile = File.createTempFile("message", ".ion", workingDirectory.toFile());
            this.records.put(stream, Pair.of(tempFile, new FileOutputStream(tempFile)));
        }

        this.recordsCount.computeIfAbsent(stream, k -> new AtomicInteger()).incrementAndGet();

        long count = this.recordsCount.values().stream().mapToLong(AtomicInteger::get).sum();

        if (count > 0 && count % 5000 == 0) {
            runContext.logger().debug("Received {} records: {}", count, this.recordsCount);
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

    private List<String> tapCommand(RunContext runContext) throws IllegalVariableEvaluationException {
        String catalogName = this.catalogName();

        return Collections.singletonList(
             "./bin/" + this.finalCommand(runContext) +
                " --config ./" + "config.json " +
                (catalogName != null ? "--" + catalogName + " ./" + catalogName + ".json " : "") +
                (this.features().contains(Feature.STATE) ? "--state ./" + "state.json" : "")
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
            title = "Raw singer streams",
            description = "Json multiline file with raw singer format that can be passed to a target"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final URI raw;

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
