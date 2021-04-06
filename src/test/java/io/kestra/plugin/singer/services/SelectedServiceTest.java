package io.kestra.plugin.singer.services;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.singer.models.DiscoverStream;
import io.kestra.plugin.singer.models.DiscoverStreams;
import io.kestra.plugin.singer.models.StreamsConfiguration;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class SelectedServiceTest {
    private static final ObjectMapper MAPPER = JacksonMapper.ofJson()
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    @Test
    void noSelection() throws IOException {
        DiscoverStreams discoverStreams = discoverStreams();

        DiscoverStreams fill = SelectedService.fill(
            discoverStreams,
            Collections.emptyList()
        );

        assertThat(fill.getStreams().size(), is(13));
        assertThat(selected(fill).count(), is(0L));
        assertThat(selectedMetadata(fill).count(), is(0L));
    }

    @Test
    void selection() throws IOException {
        DiscoverStreams discoverStreams = discoverStreams();

        DiscoverStreams fill = SelectedService.fill(
            discoverStreams,
            Collections.singletonList(StreamsConfiguration.builder()
                .stream("customer")
                .build()
            )
        );

        assertThat(fill.getStreams().size(), is(13));
        assertThat(selected(fill).count(), is(1L));
        assertThat(selectedMetadata(fill).count(), is(12L));
    }

    @Test
    void selectionProperties() throws IOException {
        DiscoverStreams discoverStreams = discoverStreams();

        DiscoverStreams fill = SelectedService.fill(
            discoverStreams,
            Arrays.asList(
                StreamsConfiguration.builder()
                    .stream("customer")
                    .selected(true)
                    .build(),
                StreamsConfiguration.builder()
                    .stream("customer")
                    .propertiesPattern(Collections.singletonList("^.*name.*$"))
                    .selected(false)
                    .build()
            )
        );

        assertThat(fill.getStreams().size(), is(13));
        assertThat(selected(fill).count(), is(1L));
        assertThat(selectedMetadata(fill).count(), is(10L));
    }


    @Test
    void selectionSimple() throws IOException {
        DiscoverStreams discoverStreams = discoverStreams();

        DiscoverStreams fill = SelectedService.fill(
            discoverStreams,
            Arrays.asList(
                StreamsConfiguration.builder()
                    .stream("customer")
                    .propertiesPattern(Collections.singletonList("^.*name.*$"))
                    .selected(true)
                    .build()
            )
        );

        assertThat(fill.getStreams().size(), is(13));
        assertThat(selected(fill).count(), is(1L));
        assertThat(selectedMetadata(fill).count(), is(2L));
    }


    @Test
    void order() throws IOException {
        DiscoverStreams discoverStreams = discoverStreams();

        DiscoverStreams fill = SelectedService.fill(
            discoverStreams,
            Arrays.asList(
                StreamsConfiguration.builder()
                    .stream("customer")
                    .selected(true)
                    .build(),
                StreamsConfiguration.builder()
                    .stream("customer")
                    .propertiesPattern(Collections.singletonList("^.*name.*$"))
                    .selected(false)
                    .build(),
                StreamsConfiguration.builder()
                    .stream("customer")
                    .selected(true)
                    .build()
            )
        );

        assertThat(fill.getStreams().size(), is(13));
        assertThat(selected(fill).count(), is(1L));
        assertThat(selectedMetadata(fill).count(), is(12L));
    }

    private DiscoverStreams discoverStreams() throws IOException {
        String properties = IOUtils.toString(
            Objects.requireNonNull(SelectedServiceTest.class.getClassLoader().getResource("properties/postgres.json")),
            StandardCharsets.UTF_8
        );

        return MAPPER.readValue(properties, DiscoverStreams.class);
    }

    private Stream<DiscoverStream> selected(DiscoverStreams discoverStreams) {
        return discoverStreams
            .getStreams()
            .stream()
            .filter(discoverStream -> discoverStream
                .getMetadata()
                .stream()
                .anyMatch(metadata -> metadata.getMetadata().isSelected())
            );
    }

    private Stream<DiscoverStream.Metadata> selectedMetadata(DiscoverStreams discoverStreams) {
        return selected(discoverStreams)
            .flatMap(discoverStream -> discoverStream.getMetadata().stream())
            .filter(metadata -> metadata.getMetadata().isSelected());
    }
}
