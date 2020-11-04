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

## Running producer

```bash
cd aiven-kafka
. env/bin/activate
python3 producer.py
```

## Running consumer

```bash
cd aiven-kafka
. env/bin/activate
python3 consumer.py
```

## Running the tests

```bash
cd aiven-kafka
. env/bin/activate

pytest
```
