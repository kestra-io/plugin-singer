package io.kestra.plugin.singer.taps;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.singer.models.streams.AbstractStream;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class ExchangeRateHostTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @SuppressWarnings("unchecked")
    @Test
    void run() throws Exception {
        ExchangeRateHost.ExchangeRateHostBuilder<?, ?> builder = ExchangeRateHost.builder()
            .id(IdUtils.create())
            .type(ExchangeRateHost.class.getName())
            .startDate(LocalDate.now().minusDays(2))
            .type(ExchangeRateHost.class.getName());

        ExchangeRateHost task = builder.build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        PipelinewiseMongoDb.Output output = task.run(runContext);

        assertThat(output.getSchemas().size(), is(1));
        assertThat(output.getSchemas().get("exchange_rate"), is(notNullValue()));
        assertThat(output.getStreams().size(), is(1));
        assertThat(output.getStreams().get("exchange_rate"), is(notNullValue()));
        assertThat(output.getState(), is(notNullValue()));

        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(output.getStreams().get("exchange_rate"))));
        List<Object> result = new ArrayList<>();
        FileSerde.reader(inputStream, result::add);

        assertThat(output.getCount().get(AbstractStream.Type.RECORD), greaterThan(0L));
        assertThat(result.size(), greaterThan(0));
    }
}
