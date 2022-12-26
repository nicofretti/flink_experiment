# Flink-experiment
 My first project using Apache Flink, a distributed stream processing framework.

### Java JDK
- Version: 11
- Vendor: Azul Zulu Community

### How to run with Docker
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
- [Apache Flink](https://nightlies.apache.org/flink/flink-docs-master/)
- Dataset word_count [QUOTES.csv](https://www.kaggle.com/datasets/coolcoder22/quotes-dataset)