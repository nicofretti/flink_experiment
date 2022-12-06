# Flink-experiment
 My first project using Apache Flink, a distributed stream processing framework.

### How to run
At first we need to start the Flink cluster. To do this, we need to run the following command:
```bash
docker-compose up
```
Now we have to access to the `jobmanager` container and run the following command in a new terminal:
```bash
docker exec -it jobmanager /bin/bash
```
It's time to run the first job. The docker infrastructure is created to add a volume from `src/` of the current directory. So, we can run the following command:
```bash
```bash
./src/run_file.sh
```