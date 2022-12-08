from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

word_count_data = ["Lorem Ipsum is simply dummy",
                   "text of the printing and typesetting industry",
                   "Lorem Ipsum has been the industry's standard",
                   "dummy text ever since the 1500s, when an unknown",
                   "printer took a galley of type and scrambled it to",
                   "make a type specimen book. It has survived not only",
                   "five centuries, but also the leap into electronic",
                   "typesetting, remaining essentially unchanged."]


def word_count(d_env: StreamExecutionEnvironment) -> None:
    # define the source
    ds = d_env.from_collection(word_count_data)

    # compute word count
    ds = ds.flat_map(split) \
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))
    # print the result
    ds.print()
    # submit for execution
    d_env.execute()


def split(line: str) -> list:
    yield from line.split()


if __name__ == '__main__':
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(1)

    word_count(env)
