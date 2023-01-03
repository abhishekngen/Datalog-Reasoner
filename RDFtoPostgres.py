from rdflib import Graph
from sqlalchemy import create_engine
import pandas as pd
import sparql_dataframe

g = Graph()
g.parse("LUBM-001.ttl")

pred_query = """
SELECT DISTINCT ?b
WHERE {
    ?a ?b ?c .
}"""

qres = g.query(pred_query)

preds = [row.b for row in qres]

for i in range(len(preds)):
    s_pred_query = """
    SELECT DISTINCT ?a ?b
    WHERE {
        ?a <""" + str(preds[i]) + """> ?b .
    }"""
    csvName = f"results_{i+1}.csv"
    g.query(s_pred_query).serialize(destination=csvName, format="csv")

    df = pd.read_csv(csvName, sep=',', index_col=False)
    df.reset_index(drop=True, inplace=True)

    engine = create_engine('postgresql://postgres:metamorphic@localhost:5433/Datalog_Sample')

    df.to_sql(f"table_{i}", engine)

    with engine.connect() as con:

        rs = con.execute(f'ALTER TABLE table_{i} DROP COLUMN index')



