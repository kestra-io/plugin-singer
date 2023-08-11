package io.kestra.plugin.singer.taps;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.services.LogService;
import io.kestra.plugin.singer.AbstractPythonSinger;
import io.kestra.plugin.singer.models.DiscoverStreams;
import io.kestra.plugin.singer.models.Feature;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import io.kestra.plugin.singer.services.SelectedService;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
        this.setup(runContext);

        // catalog or properties
        if (this.features().contains(Feature.PROPERTIES) || this.features().contains(Feature.CATALOG)) {
            DiscoverStreams discoverProperties = this.discover(runContext, this.finalCommand(runContext));
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
        // prepare
        this.initEnvDiscoveryAndState(runContext);

        // sync
        Long itemsCount = runSync(runContext);

        // metrics
        runContext.metric(Counter.of("RAW", itemsCount));

        this.saveSingerMetrics(runContext);
        runContext.logger().info("Ended singer with {} raw items", itemsCount);

        if(this.rawSingerStream == null) {
            this.rawData("");
        }

        Output.OutputBuilder outputBuilder = Output.builder()
            .count(itemsCount)
            .raw(runContext.putTempFile(this.rawSingerStream.getLeft()));

        if(this.features().contains(Feature.STATE)) {
            this.saveState(runContext);
        }

        return outputBuilder
            .build();
    }

    @SuppressWarnings("unchecked")
    private Long runSync(RunContext runContext) {
        Flowable<String> flowable = Flowable.create(
            emitter -> {
                this.run(runContext, this.tapCommand(runContext), new SingerLogDispatcher(runContext, metrics, null));

                try (BufferedReader reader = new BufferedReader(new FileReader(this.workingDirectory.resolve("raw.jsonl").toFile()))) {
                    reader.lines().forEach(emitter::onNext);
                }

                emitter.onComplete();
            },
            BackpressureStrategy.BUFFER
        );

        return flowable
            .doOnNext(this::rawData)
            .doOnNext(line -> {
                Map<String, Object> parsed = MAPPER.readValue(line, TYPE_REFERENCE);

                if(parsed.getOrDefault("type", "UNKNOWN").equals("STATE")){
                    this.stateMessage((Map<String, Object>) parsed.get("value"));
                }
            })
            .count()
            .blockingGet();
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
            LogService.defaultLogSupplier(runContext)
        );

        DiscoverStreams discoverStreams = MAPPER.readValue(workingDirectory.resolve(discoverFileName).toFile(), DiscoverStreams.class);

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
