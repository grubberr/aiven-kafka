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
