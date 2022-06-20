from neo4j import GraphDatabase
import faker
import uuid

CITUS_URI = "clefable.fib.upc.edu:9700"
CITUS_DB = "bdm"
CITUS_USER = "postgres"
CITUS_PASSWORD = "postgres"

MOCKED_TRANSACTIONS_NUMBER = 10000

def generate_mocked_transaction_command():
    id = str(uuid.uuid4())
    origin = fake.country_code('alpha-3')
    destination = fake.country_code('alpha-3')
    description = ""
    price = fake.pyint()
    quantity = fake.pyint()
    transaction_date = fake.date()
    command = f"""MERGE (origin:Country {{name:"{origin}"}})\n\
    MERGE (destination:Country {{name:"{destination}"}})\n\
    CREATE (origin)-[r:transaction {{id: "{id}", description:"{description}", \
        price:{price}, quantity:{quantity}, unit:"kg", date:date("{transaction_date}")}}]->(destination)"""
    return command


uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))
fake = faker.Faker()

with driver.session() as session:
    for i in range(MOCKED_TRANSACTIONS_NUMBER):
        command = generate_mocked_transaction_command()
        print(f"Saving the {i}-th generated transaction...")
        session.run(command)
    
driver.close()

''' HOW TO AUTOMATICALLY MOVE DATA FROM CITRUS TO NEO4J'''
# install apoc plugin from Neo4J desktop
# follow this guide: https://neo4j.com/labs/apoc/4.3/database-integration/load-jdbc/ but mainly the following two steps:
    # download the postgresql jar driver
    # put it in the "Plugin" folder of the project
# run the following query to move the data from citrus to neo4j (TODO change the query on transactions to split the work)
"""
WITH "jdbc:postgresql://clefable.fib.upc.edu:9700/bdm?user=postgres&password=postgres" as url,
     //"select * from transactions where transaction_date between '1900-01-01' and '2100-12-31'" as query
     "transactions" as query
CALL apoc.load.jdbc(url,query) YIELD row
MERGE (origin:Country {name:row.origin})
MERGE (destination:Country {name:row.destination})
CREATE (origin)-[r:transaction {id: row.id, description:row.description, price:row.price, quantity:row.quantity, unit:row.unit, date:row.transaction_date}]->(destination)
"""


