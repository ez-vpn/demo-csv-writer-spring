# IOhub Tutorial Project - CSV writer

## Spring Boot version

Collects values from mqtt broker and save values on a csv file.

**This project is intended as a tutorial for custom IOhub containers development.**

In a real production environment you should take care of the csv max file size, probably rotating created csv files, to avoid to fill up the available file system.

## Environment variables

- `MQTT_HOST`: mqtt host. Defaults to `127.0.0.1`.
- `MQTT_PORT`: mqtt port. Defaults to `1883`.
- `MQTT_IN_TOPIC`: mqtt topics to subscribe. Default to `fld/+/r/#`.
- `CSV_PATH`: File path of csv file to be populated with mqtt incoming messages. Defaults to `/tmp/log.csv`.

## Compile project

```bash
./mvnw clean package
``````

## Run project locally

```bash
java -jar target/demo-csv-writer-1.0.0.jar
``````

## Docker build

You can build and push on docker hub your image using the commands below. Otherwise just adapt the image name to your Docker Hub account.

```bash
DOCKER_HUB_ACCOUNT=<your docker hub account>
IMG_NAME=demo-csv-writer
IMG_VERSION=1.0.0
docker build -t ${DOCKER_HUB_ACCOUNT}/${IMG_NAME} -t ${DOCKER_HUB_ACCOUNT}/${IMG_NAME}:${IMG_VERSION} .
docker push ${DOCKER_HUB_ACCOUNT}/${IMG_NAME}
```
