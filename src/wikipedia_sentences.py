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
                .column('sentence_id', DataTypes.STRING())
                .column('episode_id', DataTypes.STRING())
                .column('season', DataTypes.STRING())
                .column('episode', DataTypes.STRING())
                .column('sentence', DataTypes.STRING())
                .build())
        .option('path', input_path)
        .format('csv')
        .build()
    )
    # Return the table created
    return env.from_path(table_name)


@udtf(result_types=[DataTypes.STRING()])
def split(line: Row):
    # 4-th element of the row is the sentence
    for s in line[4].split():
        yield Row(s)


if __name__ == "__main__":
    # Define files path from the current directory
    file_input = "datasets/sentences.csv"
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
        .select(col('word'), lit(1).count) \
        .to_pandas().to_csv(os.path.join(current_dir, file_output), index=False, header=False)
    #source_table.to_pandas().to_csv(os.path.join(current_dir, file_output), index=False, header=False)