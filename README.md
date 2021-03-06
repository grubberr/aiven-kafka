# aiven-kafka

Checker system that monitors website availability over the
network, produces metrics about this and passes these events through an Aiven
Kafka instance into an Aiven PostgreSQL database.

## Installation

```bash
git clone git@github.com:grubberr/aiven-kafka.git
cd aiven-kafka
python3 -m venv env
. env/bin/activate
pip3 install poetry
poetry install
```

Create a `.env` file in the root directory of your project. Add
environment-specific variables for PostgreSQL database and Kafka instance.
For example:

```dosini
DATABASE_URL=postgres://localhost/database
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

For Kafka instance you need to download all required credentials files and put them in your project root:
1. CA Certificate: ca.pem
2. Access Certificate: service.cert
3. Access Key: service.key

You need to create a topic on your Kafka cluster you can use default name 'topic1'
(or re-define this name using KAFKA_TOPIC setting param)

For PostgreSQL database you need to create database only (no any schemas)

## Running producer

Producer instance (daemon) fetch urls from `urls.yaml` file. It's yaml file which allows to define urls for checking.
You can defined just list of urls.

```yaml
- url1
- url2
```

For some urls for which you want to have more control
you can to define additinal settings:

```yaml
- url: url3
  delay: 120
  regex: find\s+me
```
1. delay - delay number of seconds between checks.
2. regex - it will try to search all ocurences of specified pattern on page.

```bash
cd aiven-kafka
. env/bin/activate
python3 producer.py
```

## Running consumer

Consumer instance (daemon) receive check results from Kafka and store them in PostgreSQL.

```bash
cd aiven-kafka
. env/bin/activate
python3 consumer.py
```

## Running the tests

For running tests you need to run local PostgreSQL instance with superuser access (user: postgres)
because tests drop and create new database for cleanup

```bash
cd aiven-kafka
. env/bin/activate

pytest
```
