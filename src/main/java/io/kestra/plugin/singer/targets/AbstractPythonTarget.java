package io.kestra.plugin.singer.targets;

import com.fasterxml.jackson.core.type.TypeReference;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.singer.AbstractPythonSinger;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractPythonTarget extends AbstractPythonSinger {
    private static final TypeReference<Map<String, Object>> TYPE_REFERENCE = new TypeReference<>() {
    };

    @Schema(
        title = "The raw data from a tap"
    )
    @NotNull
    @Valid
    private String from;

    protected AbstractPythonTarget.Output runTarget(RunContext runContext) throws Exception {
        this.setup(runContext);

        // from
        URI from = new URI(runContext.render(this.from));
        Path tempFile = runContext.tempFile();
        Files.copy(runContext.uriToInputStream(from), tempFile, StandardCopyOption.REPLACE_EXISTING);

        // sync
        this.tapsSync(tempFile, runContext);
        this.saveSingerMetrics(runContext);

        // outputs
        AbstractPythonTarget.Output.OutputBuilder builder = AbstractPythonTarget.Output.builder();

        if (!this.stateRecords.isEmpty()) {
            builder.state(this.saveState(runContext, this.stateName, this.stateRecords));
        }

        return builder.build();
    }

    protected void tapsSync(Path tempFile, RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> commands = new ArrayList<>(List.of("cat " + tempFile.toAbsolutePath()));

        commands.add("|");
        commands.add("./bin/" + this.finalCommand(runContext) + " --config config.json");

        this.runSinger(commands, runContext);
    }

    protected void runSinger(List<String> commands, RunContext runContext) {
        Flowable
            .<String>create(
                emitter -> {
                    this.run(
                        runContext,
                        String.join(" ", commands),
                        new SingerLogDispatcher(runContext, metrics, emitter::onNext)
                    );

                    emitter.onComplete();
                },
                BackpressureStrategy.BUFFER
            )
            .map(s -> MAPPER.readValue(s, TYPE_REFERENCE))
            .blockingForEach(this::stateMessage);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Uri of the state file"
        )
        private final URI state;
    }
}
