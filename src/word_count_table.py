import os
from pyflink.common import Row
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes)
from pyflink.table.expressions import lit, col
from pyflink.table.udf import udtf


def create_source_table(env, table_name, input_path):
    # Set up the config of the table `table_name`
    env.create_temporary_table(
        table_name,
        TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('author', DataTypes.STRING())
                .column('quote', DataTypes.STRING())
                .build())
        .option('path', input_path)
        .option('csv.field-delimiter', ',')
        .format('csv')
        .build()
    )
    # Return the table created
    return env.from_path(table_name)


@udtf(result_types=[DataTypes.STRING()])
def split(line: Row):
    # 1-th element of the row is the quote
    for s in line[1].split():
        s = s.lower()
        for char in ['"', ',', ';', '.', '?', '!', '(', ')', ':']:  # Remove punctuation
            s = s.replace(char, '')
        yield Row(s)


if __name__ == "__main__":
    # Define files path from the current directory
    file_input = "datasets/QUOTE.csv"
    file_output = "output/result.csv"
    # Get the current directory
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # Set up the input and output path
    file_path = os.path.join(current_dir, file_input)
    # Set up the environment
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.get_config().set("parallelism.default", "1")
    # Create source table
    source_table = create_source_table(t_env, 'source', file_path)
    # Executing the word count
    source_table.flat_map(split).alias('word') \
        .group_by(col('word')) \
        .select(col('word'), lit(1).count.alias('count')) \
        .order_by(col('count').desc) \
        .to_pandas().to_csv(os.path.join(current_dir, file_output), index=False, header=False)
