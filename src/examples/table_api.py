from pyflink.common import Row
from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes)
from pyflink.table.expressions import lit, col
from pyflink.table.udf import udtf

word_count_data = ["Lorem Ipsum is simply dummy",
                   "text of the printing and typesetting industry",
                   "Lorem Ipsum has been the industry's standard",
                   "dummy text ever since the 1500s, when an unknown",
                   "printer took a galley of type and scrambled it to",
                   "make a type specimen book. It has survived not only",
                   "five centuries, but also the leap into electronic",
                   "typesetting, remaining essentially unchanged."]


def word_count(t_env: TableEnvironment) -> None:
    tab = t_env.from_elements(map(lambda i: (i,), word_count_data),
                              DataTypes.ROW([DataTypes.FIELD('line', DataTypes.STRING())]))

    t_env.create_temporary_table(
        'sink',
        TableDescriptor.for_connector('print')
        .schema(Schema.new_builder()
                .column('word', DataTypes.STRING())
                .column('count', DataTypes.BIGINT())
                .build())
        .build())

    # compute word count
    tab.flat_map(split).alias('word') \
        .group_by(col('word')) \
        .select(col('word'), lit(1).count) \
        .execute_insert('sink') \
        .wait()


@udtf(result_types=[DataTypes.STRING()])
def split(line: Row) -> list:
    for s in line[0].split():
        yield Row(s)


if __name__ == '__main__':
    env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    # write all the data to one file
    env.get_config().set("parallelism.default", "1")
    word_count(env)
