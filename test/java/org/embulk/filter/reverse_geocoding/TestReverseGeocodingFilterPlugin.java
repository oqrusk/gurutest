package org.embulk.filter.reverse_geocoding;

import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.filter.reverse_geocoding.ReverseGeoCodingFilterPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.STRING;
import static org.junit.Assert.assertEquals;

public class TestReverseGeocodingFilterPlugin
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    private ReverseGeoCodingFilterPlugin plugin;

    @Before
    public void createResource()
    {
        plugin = new ReverseGeoCodingFilterPlugin();
    }

    private Schema schema(Column... columns)
    {
        return new Schema(Lists.newArrayList(columns));
    }

    private ConfigSource configFromYamlString(String... lines)
    {
        StringBuilder builder = new StringBuilder();
        for (String line : lines) {
            builder.append(line).append("\n");
        }
        String yamlString = builder.toString();

        ConfigLoader loader = new ConfigLoader(Exec.getModelManager());
        return loader.fromYamlString(yamlString);
    }

    private ReverseGeoCodingFilterPlugin.PluginTask taskFromYamlString(String... lines)
    {
        ConfigSource config = configFromYamlString(lines);
        return config.loadConfig(ReverseGeoCodingFilterPlugin.PluginTask.class);
    }

    private void transaction(ConfigSource config, Schema inputSchema)
    {
        plugin.transaction(config, inputSchema, new FilterPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
            }
        });
    }

    @Test
    public void test_configure_columns_option()
    {
        PluginTask task = taskFromYamlString(
                "type: reverse_geocoding",
                "target_lon: lon",
                "target_lat: lat",
                "level: 5",
                "output_columns:",
                "  - {name: pref, type: string, out: pref}",
                "  - {name: city, type: string, out: city}");
        assertEquals(2, task.getOutputColumns().size());
    }

    @Test
    public void test_buildOutputSchema()
    {
        PluginTask task = taskFromYamlString(
                "type: reverse_geocoding",
                "target_lon: lon",
                "target_lat: lat",
                "level: 5",
                "output_columns:",
                "  - {name: pref, type: string, out: pref}",
                "  - {name: city, type: string, out: city}");

        final Schema inputSchema = Schema.builder()
                .add("lat", DOUBLE)
                .add("lon", DOUBLE)
                .build();

        int res = inputSchema.getColumnCount() + 2;

        Schema outputSchema = ReverseGeoCodingFilterPlugin.buildOutputSchema(task, inputSchema);

        assertEquals(res, outputSchema.size());

        Column column;
        {
            column = outputSchema.getColumn(2);
            assertEquals("pref", column.getName());
            assertEquals(STRING, column.getType());
        }
        {
            column = outputSchema.getColumn(3);
            assertEquals("city", column.getName());
            assertEquals(STRING, column.getType());
        }
    }

    @Test
    public void output_convert_data()
    {
        ConfigSource config = configFromYamlString(
                "type: reverse_geocoding",
                "target_lon: lon",
                "target_lat: lat",
                "level: 5",
                "output_columns:",
                "  - {name: pref, type: string, out: pref}",
                "  - {name: city, type: string, out: city}",
                "  - {name: hash, type: string, out: hash}");

        final Schema inputSchema = Schema.builder()
                .add("lat", DOUBLE)
                .add("lon", DOUBLE)
                .build();

        plugin = new ReverseGeoCodingFilterPlugin();
        plugin.transaction(config, inputSchema, new FilterPlugin.Control()
        {
            @Override
            public void run(TaskSource taskSource, Schema outputSchema)
            {
                TestPageBuilderReader.MockPageOutput mockPageOutput = new TestPageBuilderReader.MockPageOutput();
                PageOutput pageOutput = plugin.open(taskSource,
                        inputSchema,
                        outputSchema,
                        mockPageOutput);

                double testLat = 35.6721277;
                double testLon = 139.75891209999997;
                for (Page page : PageTestUtils.buildPage(runtime.getBufferAllocator(),
                        inputSchema, testLat, testLon)) {
                    pageOutput.add(page);
                }

                pageOutput.finish();
                pageOutput.close();

                PageReader pageReader = new PageReader(outputSchema);

                for (Page page : mockPageOutput.pages) {
                    pageReader.setPage(page);
                    assertEquals(testLat, pageReader.getDouble(outputSchema.getColumn(0)), 1);
                    assertEquals(testLon, pageReader.getDouble(outputSchema.getColumn(1)), 1);
                    assertEquals("東京都", pageReader.getString(outputSchema.getColumn(2)));
                    assertEquals("東京都中央区", pageReader.getString(outputSchema.getColumn(3)));
                }
            }
        });
    }

//    @Test(expected = ConfigException.class)
//    public void configure_EitherOfColumnsOrDropColumnsCanBeSpecified()
//    {
//        ConfigSource config = configFromYamlString(
//                "type: column",
//                "columns:",
//                "- {name: a}",
//                "drop_columns:",
//                "- {name: a}");
//        Schema inputSchema = schema(
//                new Column(0, "a", STRING),
//                new Column(1, "b", STRING));
//
//        transaction(config, inputSchema);
//    }
}
