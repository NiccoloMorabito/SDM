// install apoc plugin from Neo4J desktop
// follow this guide: https://neo4j.com/labs/apoc/4.3/database-integration/load-jdbc/ but mainly the following two steps:
// download the postgresql jar driver
// put it in the "Plugin" folder of the project
// run the following query to move the data from citrus to neo4j (TODO change the query on transactions to split the work)

WITH "jdbc:postgresql://clefable.fib.upc.edu:9700/bdm?user=postgres&password=postgres" as url,
     "select * from transactions
     where product_category='1201'
        and transaction_date between '1900-01-01' and '2100-12-31'" as query
CALL apoc.load.jdbc(url,query) YIELD row
MERGE (origin:Country {name:row.origin})
MERGE (destination:Country {name:row.destination})
CREATE (origin)-[r:transaction {id: row.id, description:row.description, price:row.price, quantity:row.quantity, unit:row.unit, date:row.transaction_date}]->(destination)

