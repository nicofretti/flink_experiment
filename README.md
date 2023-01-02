# Flink-experiment
This project is an experiment with Apache Flink, a framework for distributed stream and batch data processing. The project is composed by two main parts:
- `src/word_count`: a word count example using PyFlink
- `src/data_expo_queries`: list of five queries taken from the Data Expo 2009 dataset, using Apache Flink with Java

Those two parts have been separated in two different folders after some difficulties with PyFlink. Apache Flink offers a lot of APIs for Python but there are still some limitations/bugs that make difficult to develop a project with it. So the queries of Data Expo 2009 dataset have been developed with Java, the most stable and supported API.

## Apache Flink
Apache Flink is an open-source, distributed processing system for bounded and unbounded data streams. It is a powerful tool for building data pipelines and processing data in real time. 
### Stream Processing
In Flink, applications are composed streaming dataflows that can be transformed using operators like `map`, `reduce`, `keyBy` etc. The dataflows form a directed acyclic graph (DAG) that start with one o more source and end with one or more sinks. The dataflows can be executed on a local machine or on a cluster of machines, and can be executed in a streaming or batch fashion.
<p align="center">
    <img src="img/parallel_dataflow.svg">
</p>

As you can see in the example we can spot two type of operators:
- `one-to-one` operators, like `map` and `filter`, that transform one input element into one output element preserving partitioning and ordering.
- `redistributing` operators, like `keyBy` or `rebalance`, that transform one input element into zero, one or more output elements

### Windowing
Flink has also the concept of windows, which allow you to process data over a fixed period of time, or you can define some custom function (like events count). Flink offers different types of built-in windows:
- `Tumbling Window`: these windows are fixed-size, non-overlapping windows that are created based on a fixed time interval or number of events. For example, a tumbling window of 5 seconds the current window will be evaluated every 5 seconds and will contain the last 5 seconds of data.
- `Sliding Window`: simular to the tumbling window but these windows are also allowed to overlap. You can specify the size of the window and the interval at which the windows slide. 
- `Session Window`: these windows group data into sessions based on the time that has passed since the last event. You can specify a maximum gap between events, and any events that fall within that gap will be placed in the same session.
- `Global Window`: a window that contains all the events in the stream.

<p align="center">
    <img src="img/windowing.svg">
</p>

### Datastream API and Table API

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