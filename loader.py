import uuid

import faker
from neo4j import GraphDatabase

CITUS_URI = "clefable.fib.upc.edu:9700"
CITUS_DB = "bdm"
CITUS_USER = "postgres"
CITUS_PASSWORD = "postgres"

MOCKED_TRANSACTIONS_NUMBER = 100000

fake = faker.Faker()


def generate_mocked_transaction():
    tid = str(uuid.uuid4())
    origin = fake.country_code('alpha-3')
    destination = fake.country_code('alpha-3')
    while origin == destination:
        destination = fake.country_code('alpha-3')
    description = ""
    price = fake.pyint()
    quantity = fake.pyint()
    transaction_date = fake.date()
    return tid, origin, destination, description, price, quantity, transaction_date


def generate_mocked_transaction_command():
    tid, origin, destination, description, price, quantity, transaction_date = generate_mocked_transaction()
    command = f"""MERGE (origin:Country {{name:"{origin}"}})\n\
    MERGE (destination:Country {{name:"{destination}"}})\n\
    CREATE (origin)-[r:transaction {{id: "{tid}", description:"{description}", \
        price:{price}, quantity:{quantity}, unit:"kg", date:date("{transaction_date}")}}]->(destination)"""
    return command


def load_fake_data_to_neo4j():
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri)

    with driver.session() as session:
        for i in range(MOCKED_TRANSACTIONS_NUMBER):
            command = generate_mocked_transaction_command()
            print(f"Saving the {i}-th generated transaction...")
            session.run(command)

    driver.close()


if __name__ == '__main__':
    load_fake_data_to_neo4j()
