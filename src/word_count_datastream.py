import os

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource
from pyflink.datastream.formats.csv import CsvReaderFormat, CsvSchema
from pyflink.table import StreamTableEnvironment


def split(line):
    # The 0-th element of the row is the quote
    row = line[0].lower()
    for char in ['"', ',', ';', '.', '?', '!', '(', ')', ':']:  # Remove punctuation
        row = row.replace(char, '')
    yield from row.split()


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.realpath(__file__))
    input_path = os.path.join(current_dir, "datasets/Quotes_data.csv")
    output_path = os.path.join(current_dir, "output")
    d_env = StreamExecutionEnvironment.get_execution_environment()
    d_env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    d_env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=d_env)

    csv_format = CsvSchema.builder() \
        .add_string_column("quote") \
        .add_string_column("author") \
        .add_string_column("type") \
        .build()

    # Read from csv file
    ds = d_env.from_source(
        source_name="file_source",
        source=FileSource.for_record_stream_format(CsvReaderFormat.for_schema(csv_format), input_path).build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps()
    )

    ds = ds.flat_map(split) \
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], (i[1] + j[1])))    # ds.print()
    t_env.from_data_stream(ds).to_pandas()\
        .to_csv(output_path, index=False, header=False)
    # csv_out = CsvSchema.builder() \
    #     .add_string_column("word") \
    #     .add_string_column("count") \
    #     .set_column_separator(",")
    # sink = FileSink.for_bulk_format(
    #     output_path,
    #     CsvBulkWriters.for_schema(csv_out.build()),
    # ).build()
    # ds.sink_to(
    #     sink=sink
    # )
    # ds.sink_to(
    #     sink=FileSink.for_row_format(
    #         base_path=output_path,
    #         encoder=Encoder.simple_string_encoder()
    #     ).with_output_file_config(
    #         OutputFileConfig.builder()
    #         .with_part_prefix("word_count")
    #         .with_part_suffix(".txt")
    #         .build()
    #     ).with_rolling_policy(
    #         RollingPolicy.default_rolling_policy()
    #     ).build())

    # Display only the first 10 elements

    d_env.execute("word_count")
