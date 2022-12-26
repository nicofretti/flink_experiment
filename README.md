# Flink-experiment
This project is a experiment with Apache Flink, a framework for distributed stream and batch data processing. The project is composed by two main parts:
- `src/word_count`: a word count example using PyFlink
- `src/data_expo_queries`: list of five queries taken from the Data Expo 2009 dataset, using Apache Flink with Java

Those two parts have been separated in two different folders after some difficulties with PyFlink. Apache Flink offers a lot of APIs for Python but there are still some limitations/bugs that make difficult to develop a project with it. So the queries of Data Expo 2009 dataset have been developed with Java, the most stable and supported API.

## Project structure
Other then the two main parts of the project, there are some other folders:
- `src/datasets`: contains the datasets used in the project, in the citation section there are the links to download them
- `src/utils`: contains some utility functions used in the project, like `data_expo_socket.py` that is used to create a socket server to send the Data Expo 2009 dataset to the Flink cluster.

To make the project easier to run, there is a `docker-compose.yml` file that creates a Flink cluster later explained in more details. Flink works with Python 3.6, 3.7 and 3.8, so there is also a `environment.yml` file to create a conda environment with the right Python version.

## Setting up the environment

The version of Apache Flink used in this project is [1.16.0](https://nightlies.apache.org/flink/flink-docs-release-1.16/), the latest stable version at the moment of writing this document. The project has been developed in a Linux environment, but it should work in other OS as well.

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