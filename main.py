from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema,
                           DataTypes)

if __name__ == "__main__":
    file_path = "./datasets/up_07.csv"
    file_format = "csv"
    # Init table environment
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "1")
    # Create sink tables
    # Year,Month,DayofMonth...
    table_descriptor = TableDescriptor.for_connector("filesystem") \
        .schema(Schema.new_builder()
                .column("Year", DataTypes.STRING()) \
                .column("Month", DataTypes.STRING()) \
                .column("DayofMonth", DataTypes.STRING()) \
                .build()) \
        .option("path", file_path) \
        .option("format", file_format) \
        .build()

    t_env.create_temporary_table(
        "transformed_table",
        table_descriptor
    )

    table = t_env.from_path("transformed_table")


