# Flink-experiment
This project is an experiment with Apache Flink, a framework for distributed stream and batch data processing. There are two main parts:
- `src/word_count`: a word count example using PyFlink
- `src/data_expo_queries`: list of four queries taken from the Data Expo 2009 dataset, using Apache Flink with Java

Those two parts have been separated in two different folders after some difficulties with PyFlink. Apache Flink offers a lot of APIs for Python but there are still some limitations that make difficult to develop a project with it. So the queries of Data Expo 2009 dataset have been developed with Java, the most stable and supported API.

## Apache Flink
Apache Flink is an open-source stream processing framework that can be used for processing unbounded and bounded data streams. It is a distributed system that can run on a cluster of machines, and it provides efficient, scalable, and fault-tolerant stream and batch data processing. Flink is written in Java and Scala, and it has a rich set of APIs for creating streaming data pipelines.

### Stream Processing
In Flink applications are composed of streaming dataflows that can be transformed using operators like `map`, `reduce`, `keyBy` etc. The dataflows form a directed acyclic graph (DAG) that start with one o more source and end with one or more sinks. The dataflows can be executed on a local machine or on a cluster of machines, and can be executed in a streaming or batch fashion.
<p align="center">
    <img src="img/parallel_dataflow.svg">
</p>

As you can see in the example we can spot two type of operators:
- `one-to-one` operators, like `map` and `filter`, that transform one input element into one output element preserving partitioning and ordering.
- `redistributing` operators, like `keyBy` or `rebalance`, that transform one input element into zero, one or more output elements

### Datastream API and Table API
In Flink there are two main programming interface for working with streams of data: `DataStream API` and `Table API`.

<p align="center">
    <img width=400 src="img/levels_of_abstraction.svg">
</p>

The DataStream API is core API for working with streams of data. It allows to process streams in real-time and perform transformations using operators like `map`, `filter`, `join`, `aggregate` etc.

```Python
data_stream = ... # create a source data stream
data_stream.flat_map(Splitter) \
    .map(lambda i: (i, 1)) \
    .key_by(lambda i: i[0]) \
    .reduce(lambda i, j: (i[0], (i[1] + j[1]))) \
    .map(lambda i: Row(word=i[0], count=i[1]),
         output_type=Types.ROW_NAMED(["word", "count"], [Types.STRING(), Types.INT()]))
```

The Table API instead is a programming interface for working with streams of data in a tabular format. It allows to express complex stream processing queries using a SQL-like syntax:

```Python
source_table = ... # create a source table
source_table.flat_map(Splitter).alias('word') \
    .group_by(col('word')) \
    .select(col('word'), lit(1).count.alias('count')) \
    .order_by(col('count').desc) \
    .print()
```

DataStream API is more generic and allows more control over streams especially with custom functions, while the Table API is more expressive and easier to use. The Table API is built on top of the DataStream API, so it is possible to convert a Table into a DataStream and vice versa.

### Windowing
Flink has also the concept of windows, which allow you to process data over a fixed period of time, or you can define some custom function (like events count). There are four type of built-in windows:
- `Tumbling Window`: these windows are fixed-size, non-overlapping windows that are created based on a fixed time interval or number of events. For example, a tumbling window of 5 seconds the current window will be evaluated every 5 seconds and will contain the last 5 seconds of data.
- `Sliding Window`: simular to the tumbling window but these windows are also allowed to overlap. You can specify the size of the window and the interval at which the windows slide. 
- `Session Window`: these windows group data into sessions based on the time that has passed since the last event. You can specify a maximum gap between events, and any events that fall within that gap will be placed in the same session.
- `Global Window`: a window that contains all the events in the stream.

<p align="center">
    <img src="img/windowing.svg">
</p>

### Flink Architecture
Flink is a distributed system that can run on a cluster of machines. It is composed by three main components:
- `JobManager`: it is the master of the cluster and is responsible for the execution of the application. It receives job submissions from clients, and then it orchestrates the execution of the job by scheduling tasks on the available TaskManagers. The JobManager also maintains the global state of the Flink cluster, including the state of running and completed jobs.
- `TaskManager`: it is the worker node of the Flink cluster and is responsible for the execution of the tasks assigned to it by the JobManager. TaskManager has a fixed number of slots, and each slot can run one task at a time. The TaskManager also manages the local state of the tasks that are running on it, and it communicates with the JobManager to report the status of its tasks.
- `Client`: it is the client that submits the application to the cluster. It is responsible for the submission of the application to the cluster, the monitoring of the execution, etc.

<p align="center">
    <img src="img/distributed_runtime.svg">
</p>

### Execution environment
The execution environment is the entry point for creating a Flink application, it is responsible for creating the data sources, sinks, and for executing the application. The execution environment is created by using the `StreamExecutionEnvironment` for DataStream API and `TableEnvironment` for Table API. Generally speaking, after defining the operation on the data source the application is executed by calling the `execute` method on the execution environment.

```Java
StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> data_stream = ... # create a source data stream
    data_stream.
        .name("source")
        .map(...).name("map1")
        .map(...).name("map2")
        .rebalance()
        .map(...).name("map3")
        .map(...).name("map4")
        .keyBy((value) -> value)
        .map(...).name("map5")
        .map(...).name("map6")
        .sinkTo(...).name("sink");

        env.execute();
```
Operations that imply 1-to-1 connection patter between operation (like `map`, `flatMap`, `filter`) can just be chained together. Operations that imply a redistribution of the data (like `keyBy`, `rebalance`) will be executed in a separate task. Following the example above, the execution environment will create 3 tasks of the given Job:
- Task 1: `source`, `map1`, `map2`
- Task 2: `map3`, `map4`
- Task 3: `map5`, `map6`, `sink`

More graphically:
<p align="center">
    <img src="img/datastream_example_job_graph.svg">
</p>

## Set up a Flink cluster
A Flink cluster is composed by a JobManager and one or more TaskManagers. During the development is possible to run a Flink Job locally on a single machine where the JobManager and TaskManager are running in the same JVM. The JobManager will have a single task slot and all tasks of the Flink job will be executed on this single task slot. This is the default mode of execution when you run a Flink application from the IDE.

### Standalone cluster
Download the `1.16.0` version of Flink from the [official website](https://www.apache.org/dyn/closer.lua/flink/flink-1.16.0/flink-1.16.0-bin-scala_2.12.tgz) and unzip it. Then, go to the `bin` folder and run the following command to start a Flink cluster:
```bash
./start-cluster.sh
```
The cluster will be started on the local machine and it will be accessible at `localhost:8081`. There is a file where you can configure ports, hostnames, slots, etc. called `conf/flink-conf.yaml`.

The cluster can be stopped by running the following command also from the `bin` folder:
```bash
./stop-cluster.sh
```

### Docker cluster
The project includes a docker-compose.yml file that enables you to run a Flink cluster on Docker, making it easier to set up on any machine. The cluster consists of a JobManager and one TaskManager with five available slots. You can modify the `docker-compose.yml` file to adjust settings such as the number of slots and TaskManagers. To start the cluster, run the following command:
```bash
docker-compose build
docker-compose up
```
The cluster will be started on the local machine and it will be accessible at `localhost:8081`, like the standalone cluster. The cluster can be stopped by running the following command:
```bash
docker-compose down
```
It was somewhat challenging to build the Docker image, as I initially couldn't find a stable and supported image for PyFlink. In fact, the `Dockerfile` includes extra instructions for installing PyFlink on top of the Flink image ([flink:1.16.0-scala_2.12-java11](https://hub.docker.com/_/flink)).

## Project structure

Other than the two main parts of the project, there are some other folders:
- `src/datasets`: contains the datasets used in the project, in the citation section there are the links to download them
- `src/utils`: contains some utility functions used in the project, like `data_expo_socket.py` that is used to create a socket server to send the Data Expo 2009 dataset to the Flink cluster.

To make the project easier to run, there is a `docker-compose.yml` file that creates a Flink cluster later explained in more details. Flink works with Python 3.6, 3.7 and 3.8, so there is also a `environment.yml` file to create a conda environment with the right Python version.

## Setting up the environment

The version of Apache Flink used in this project is [1.16.0](https://nightlies.apache.org/flink/flink-docs-release-1.16/), the latest stable version at the moment of writing this document. The project has been developed in a Linux environment, but it should work in other OS as well.

### Download datasets
The datasets used in the project are:
- [Data Expo 2009](https://community.amstat.org/jointscsg-section/dataexpo/dataexpo2009): `2005.csv`, `2006.csv`, `2007.csv` and `plane-data.csv`
- [QUOTES](https://www.kaggle.com/datasets/coolcoder22/quotes-dataset): `QUOTES.csv` the dataset used in the word count example
After downloading the datasets and extracting them, we need to move them to the `src/datasets` folder, it should look like this:
```bash
├── src
│   ├── datasets
│   │   ├── 2005.csv
│   │   ├── 2006.csv
│   │   ├── 2007.csv
│   │   ├── plane-data.csv
│   │   └── QUOTES.csv
...
```

### Python with Conda
The Pyflink API support only some version of Python, so getting the right version is important. I have use Conda to create a virtual environment with the right Python version. To create the environment, run the following command:
```bash
conda env create -f environment.yml
```
Now a new environment called `flink-experiment` is created. To activate it, run the following command:
```bash
conda activate flink-experiment
```
We can now install the requirements to go on with the project:
```bash
pip install -r requirements.txt
```

### Java JDK
The Java API of Apache Flink is the most stable and supported, so it's the one used in the queries of the Data Expo 2009 dataset. To run the queries, we need to install the Java JDK. I have used the Azul Zulu Community version, but any other version should work, as long as it's Java 11.
```yaml
Version: 11
Vendor: Azul Zulu Community
```
Opening the project `src/data_expo_queries` in IntelliJ IDEA, we can see that the project is already configured to use the JDK installed in the system. If we want to use a different version, we can change it in the `Project Structure` of IntelliJ IDEA. The dependencies are managed with Gradle, so we don't need to install anything else. If there are some problems with the Gradle there are some suggestions in the Flink documentation [here](https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/configuration/gradle/).

### How to run Docker
At first we need to start the Flink cluster, for this we can use the following command:
```bash
docker-compose up
```
Now we have to access to the `jobmanager` container running the following command in a new terminal:
```bash
docker exec -it flink-jobmanager /bin/bash
```
It's time to run the first job. The docker infrastructure is created to add a volume from `src/` of the current directory. So, we can run the following command:
```bash
./src/run_file.sh
```

### References
- [Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/) framework documentation
- [QUOTES.csv](https://www.kaggle.com/datasets/coolcoder22/quotes-dataset) the dataset used in the word count example
- [Data Expo 2009](https://community.amstat.org/jointscsg-section/dataexpo/dataexpo2009) the dataset used in the queries

TODO:
- [] Filter socket with only the data we need