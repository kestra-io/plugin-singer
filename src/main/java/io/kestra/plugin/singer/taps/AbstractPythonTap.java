package io.kestra.plugin.singer.taps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.DefaultLogConsumer;
import io.kestra.core.runners.RunContext;
import io.kestra.core.storages.StorageContext;
import io.kestra.plugin.singer.AbstractPythonSinger;
import io.kestra.plugin.singer.models.DiscoverStreams;
import io.kestra.plugin.singer.models.Feature;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.services.SelectedService;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPythonTap extends AbstractPythonSinger implements RunnableTask<AbstractPythonTap.Output> {
    @Getter(AccessLevel.NONE)
    protected transient Pair<File, OutputStream> rawSingerStream;

    @Schema(
        title = "The list of stream configurations"
    )
    @PluginProperty
    @NotNull
    @NotEmpty
    @Valid
    protected List<StreamsConfiguration> streamsConfigurations;

    @Getter(value = AccessLevel.NONE)
    @Builder.Default
    private transient Map<String, AtomicInteger> recordsCount = new ConcurrentHashMap<>();

    abstract public List<Feature> features();

    public void initEnvDiscoveryAndState(RunContext runContext) throws Exception {
        // catalog or properties
        if (this.features().contains(Feature.PROPERTIES) || this.features().contains(Feature.CATALOG)) {
            DiscoverStreams discoverProperties = this.discover(runContext, this.finalCommand(runContext));
            this.writeSingerFiles(this.catalogName() + ".json", discoverProperties);
        }

        // state
        if (this.features().contains(Feature.STATE)) {
            try {
                InputStream taskStateFile = runContext.stateStore().getState(
                    runContext.render(this.stateName),
                    "state.json",
                    runContext.storage().getTaskStorageContext().map(StorageContext.Task::getTaskRunValue).orElse(null)
                );
                this.writeSingerFiles("state.json", IOUtils.toString(taskStateFile, StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                this.writeSingerFiles("state.json", "{}");
            }
        }
    }

    public Output run(RunContext runContext) throws Exception {
        // prepare
        this.initEnvDiscoveryAndState(runContext);

        // sync
        Long itemsCount = runSync(runContext);

        // metrics
        runContext.metric(Counter.of("records", itemsCount));

        this.saveSingerMetrics(runContext);
        runContext.logger().info("Ended singer with {} raw items", itemsCount);

        if (this.rawSingerStream == null) {
            this.rawData("");
        }

        Output.OutputBuilder outputBuilder = Output.builder()
            .count(itemsCount)
            .raw(runContext.storage().putFile(this.rawSingerStream.getLeft()));

        if (this.features().contains(Feature.STATE)) {
            this.saveState(runContext, runContext.render(this.stateName), this.stateRecords);
        }

        return outputBuilder
            .build();
    }

    @SuppressWarnings("unchecked")
    private Long runSync(RunContext runContext) throws Exception {
        Flux<String> flowable = Flux.create(
            throwConsumer(emitter -> {
                this.run(runContext, this.tapCommand(runContext), new SingerLogDispatcher(runContext, metrics, null));

                try (BufferedReader reader = new BufferedReader(new FileReader(this.workingDirectory.resolve("raw.jsonl").toFile()))) {
                    reader.lines().forEach(emitter::next);
                }

                emitter.complete();
            }),
            FluxSink.OverflowStrategy.BUFFER
        );

        return flowable
            .doOnNext(throwConsumer(this::rawData))
            .doOnNext(throwConsumer(line -> {
                Map<String, Object> parsed = MAPPER.readValue(line, TYPE_REFERENCE);

                if (parsed.getOrDefault("type", "UNKNOWN").equals("STATE")) {
                    this.stateMessage((Map<String, Object>) parsed.get("value"));
                }
            }))
            .count()
            .block();
    }

    public void rawData(String raw) throws IOException {
        if (this.rawSingerStream == null) {
            File tempFile = File.createTempFile("message", ".json", workingDirectory.toFile());
            this.rawSingerStream = Pair.of(tempFile, new FileOutputStream(tempFile));
        }

        this.rawSingerStream.getRight().write((raw + "\n").getBytes(StandardCharsets.UTF_8));
    }

    protected DiscoverStreams discover(RunContext runContext, String command) throws Exception {
        String discoverFileName = "discover.json";
        this.run(
            runContext,
            "./bin/" + command + " --config config.json --discover > " + discoverFileName,
            new DefaultLogConsumer(runContext)
        );

        DiscoverStreams discoverStreams = MAPPER.readValue(
            workingDirectory.resolve(discoverFileName).toFile(),
            DiscoverStreams.class
        );

        return SelectedService.fill(discoverStreams, this.streamsConfigurations);
    }

    protected String catalogName() {
        return this.features().contains(Feature.CATALOG) ? "catalog" :
            (this.features().contains(Feature.PROPERTIES) ? "properties" : null);
    }

    private String tapCommand(RunContext runContext) throws IllegalVariableEvaluationException {
        String catalogName = this.catalogName();

        return "./bin/" + this.finalCommand(runContext) +
            " --config ./" + "config.json " +
            (catalogName != null ? "--" + catalogName + " ./" + catalogName + ".json " : "") +
            (this.features().contains(Feature.STATE) ? "--state state.json" : "") +
            " > raw.jsonl";
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Raw singer streams",
            description = "Json multiline file with raw singer format that can be passed to a target"
        )
        @PluginProperty(additionalProperties = URI.class)
        private final URI raw;

        @Schema(
            title = "Counter of stream items"
        )
        private final Long count;
    }
}
