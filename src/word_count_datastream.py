import os
from pyflink.common import WatermarkStrategy, Types, Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, FileSink
from pyflink.datastream.formats.csv import CsvSchema, CsvBulkWriters, CsvReaderFormat
from pyflink.table import DataTypes


def split(line):
    # The 1-th element of the row is the quote
    row = line[1].lower()
    for char in ['"', ',', ';', '.', '?', '!', '(', ')', ':']:  # Remove punctuation
        row = row.replace(char, '')
    yield from row.split()


if __name__ == "__main__":
    # Init current directory
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # Init input and output path
    input_path = os.path.join(current_dir, "datasets/QUOTE.csv")
    output_path = os.path.join(current_dir, "output/")
    # Init environment
    d_env = StreamExecutionEnvironment.get_execution_environment()
    d_env.set_runtime_mode(RuntimeExecutionMode.BATCH)

    # Read from csv file
    csv_input_schema = CsvSchema.builder() \
        .add_string_column("author") \
        .add_string_column("quote") \
        .build()
    ds = d_env.from_source(
        source_name="file_source",
        source=FileSource.for_record_stream_format(CsvReaderFormat.for_schema(csv_input_schema), input_path).build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps()
    )
    # Making the word count and name the output columns
    ds = ds.flat_map(split) \
        .map(lambda i: (i, 1)) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], (i[1] + j[1]))) \
        .map(lambda i: Row(word=i[0], count=i[1]),
             output_type=Types.ROW_NAMED(["word", "count"], [Types.STRING(), Types.INT()]))
    # Write to csv file
    csv_output_schema = CsvSchema.builder() \
        .add_string_column("word") \
        .add_number_column("count", number_type=DataTypes.INT()) \
        .build()
    # Sink the result
    ds.sink_to(FileSink.for_bulk_format(
        output_path,
        CsvBulkWriters.for_schema(csv_output_schema)).build()
    )
    d_env.execute()

# 6m 15s