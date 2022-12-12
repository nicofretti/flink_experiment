# Flink-experiment
 My first project using Apache Flink, a distributed stream processing framework.

### How to run
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
- [Quotes - 500k](https://www.kaggle.com/datasets/manann/quotes-500k)