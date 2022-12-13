import os

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.table import StreamTableEnvironment


def split(line):
    # The 0-th element of the row is the quote
    row = line.split(",")[0].lower()
    for char in ['"', ',', ';', '.', '?', '!', '(', ')', ':']:  # Remove punctuation
        row = row.replace(char, '')
    yield from row.split()


if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.realpath(__file__))
    input_path = os.path.join(current_dir, "datasets/Quotes_data.csv")
    output_path = os.path.join(current_dir, "output/result.csv")
    d_env = StreamExecutionEnvironment.get_execution_environment()
    d_env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    d_env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=d_env)

    ds = d_env.from_source(
        source=FileSource.for_record_stream_format(StreamFormat.text_line_format(), input_path)
        .process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )

    ds = ds.flat_map(split) \
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], (i[1] + j[1])))
    # Ordering the result by the count
    t_env.from_data_stream(ds).to_pandas()\
        .to_csv(output_path, index=False, header=False)

    d_env.execute("word_count")
