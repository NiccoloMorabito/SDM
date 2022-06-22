:auto USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///data_agg.csv' AS row
MERGE (origin:Country {code:row.origin})

:auto USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///data_agg.csv' AS row
MERGE (destination:Country {code:row.destination})

:auto USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM 'file:///data_agg.csv' AS row
MATCH (origin:Country {code:row.origin})
MATCH (destination:Country {code:row.destination})
CREATE (origin)-[r:transaction {price:row.price, quantity:row.quantity}]->(destination)