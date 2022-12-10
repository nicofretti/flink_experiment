from pyflink.common import Row
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes, FormatDescriptor)
from pyflink.table.expressions import lit, col
from pyflink.table.udf import udtf


def create_source_table(env, table_name, input_path):
    # Set up the config of the table `table_name`
    env.create_temporary_table(
        table_name,
        TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('sentence', DataTypes.STRING())
                .build())
        .option('path', input_path)
        .format('raw')
        .build()
    )
    # Return the table created
    return env.from_path(table_name)


def create_sink_table(env, table_name, output_path):
    # Only create the table sink to put the result of our query
    env.create_temporary_table(
        table_name,
        TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .column('count', DataTypes.BIGINT())
                .build())
        .option('path', output_path)
        .format('csv')
        .build())


@udtf(result_types=[DataTypes.STRING()])
def split(line: Row):
    for s in line[0].split():
        yield Row(s)


if __name__ == "__main__":
    file_path = "file:///opt/flink/src/datasets/task.txt"
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.get_config().set("parallelism.default", "1")
    # Create source table
    source_table = create_source_table(t_env, 'source', file_path)
    # Create sink table, we can refer to it later using the name `sink`
    create_sink_table(t_env, 'sink', 'file:////opt/flink/src')
    # Executing word count
    source_table.flat_map(split).alias('word') \
        .group_by(col('word')) \
        .select(col('word'), lit(1).count) \
        .execute_insert('sink') \
        .wait()
