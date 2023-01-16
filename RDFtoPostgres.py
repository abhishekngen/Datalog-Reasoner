from rdflib import Graph
from sqlalchemy import create_engine
import pandas as pd
import sparql_dataframe
import pickle

class RDFtoPostgres:

    def populateDatabase(self):
        g = Graph()
        g.parse("LUBM-001.ttl")

        typePredName = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        type_object_query = """
        SELECT DISTINCT ?c
        WHERE {
            ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?c .
        }"""

        tqres = g.query(type_object_query)
        typeObjects = [row.c for row in tqres]
        f = open("LUBM_001_TypeObjects.pkl", "wb")
        pickle.dump(typeObjects, f)
        f.close()

        pred_query = """
        SELECT DISTINCT ?b
        WHERE {
            ?a ?b ?c .
        }"""

        qres = g.query(pred_query)
        # preds = [row.b for row in qres if "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" not in row.b]
        preds = [row.b for row in qres if "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" != str(row.b)]
        print(len(preds))
        f2 = open("LUBM_001_Predicates.pkl", "wb")
        pickle.dump(preds, f2)
        f2.close()


        # for i in range(len(preds)):
        #     s_pred_query = """
        #     SELECT DISTINCT ?a ?b
        #     WHERE {
        #         ?a <""" + str(preds[i]) + """> ?b .
        #     }"""
        #     pred_name = str(preds[i]).split("#")[1].strip()
        #     csvName = f"{pred_name}.csv"
        #     print(csvName)
        #     g.query(s_pred_query).serialize(destination=csvName, format="csv")

        #     df = pd.read_csv(csvName, sep=',', index_col=False)
        #     df.reset_index(drop=True, inplace=True)

        #     engine = create_engine('postgresql://postgres:metamorphic@localhost:5433/Datalog_Sample')

        #     df.to_sql(f"{pred_name}", engine)

        #     with engine.connect() as con:

        #         rs = con.execute(f'ALTER TABLE "{pred_name}" DROP COLUMN index;')

        # for i in range(len(typeObjects)):
        #     s_pred_query = """
        #     SELECT DISTINCT ?a
        #     WHERE {
        #         ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <""" + str(typeObjects[i]) + """> .
        #     }"""
        #     pred_name = str(typeObjects[i]).split("#")[1].strip()
        #     csvName = f"{pred_name}.csv"
        #     print(csvName)

        #     g.query(s_pred_query).serialize(destination=csvName, format="csv")

        #     df = pd.read_csv(csvName, sep=',', index_col=False)
        #     df.reset_index(drop=True, inplace=True)

        #     engine = create_engine('postgresql://postgres:metamorphic@localhost:5433/Datalog_Sample')

        #     df.to_sql(f"{pred_name}", engine)

        #     with engine.connect() as con:

        #         rs = con.execute(f'ALTER TABLE "{pred_name}" DROP COLUMN index;')

    def parseDatalogTextFile(self, fileName):
        f = open(fileName, "r")
        newRules = []
        for rule in f:
            if(rule.strip() != ""):
                newRule = rule.strip()[0:len(rule.strip())-2]
                newRule = "'" + newRule.replace("a1:", "") + "'"
                newRule = newRule.replace("[", "(")
                newRule = newRule.replace("]", ")")
                print(newRule)
                newRules.append(newRule)
        f.close()
        fw = open(fileName, "w")
        fw.write('\n'.join(newRules))
        fw.close()

    def populateDatabaseFromPickle(self, database):

        engine = create_engine(f'postgresql://postgres:metamorphic@localhost:5433/{database}')

        with engine.connect() as con:            
            rs = con.execute(f'DROP SCHEMA public CASCADE;')
            rs = con.execute(f'CREATE SCHEMA public;')

        f = open("LUBM_001_TypeObjects.pkl", "rb")
        typeObjects = pickle.load(f)
        f.close()

        f2 = open("LUBM_001_Predicates.pkl", "rb")
        preds = pickle.load(f2)
        f2.close()

        for i in range(len(preds)):
            # s_pred_query = """
            # SELECT DISTINCT ?a ?b
            # WHERE {
            #     ?a <""" + str(preds[i]) + """> ?b .
            # }"""
            pred_name = str(preds[i]).split("#")[1].strip()
            csvName = f"{pred_name}.csv"
            # print(csvName)
            # g.query(s_pred_query).serialize(destination=csvName, format="csv")

            df = pd.read_csv(csvName, sep=',', index_col=False)
            df.reset_index(drop=True, inplace=True)

            

            df.to_sql(f"{pred_name}", engine)

            with engine.connect() as con:

                rs = con.execute(f'ALTER TABLE "{pred_name}" DROP COLUMN index;')

        for i in range(len(typeObjects)):
            # s_pred_query = """
            # SELECT DISTINCT ?a
            # WHERE {
            #     ?a <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <""" + str(typeObjects[i]) + """> .
            # }"""
            pred_name = str(typeObjects[i]).split("#")[1].strip()
            csvName = f"{pred_name}.csv"
            # print(csvName)

            # g.query(s_pred_query).serialize(destination=csvName, format="csv")

            df = pd.read_csv(csvName, sep=',', index_col=False)
            df.reset_index(drop=True, inplace=True)

            # engine = create_engine(f'postgresql://postgres:metamorphic@localhost:5433/{database}')

            df.to_sql(f"{pred_name}", engine)

            with engine.connect() as con:

                rs = con.execute(f'ALTER TABLE "{pred_name}" DROP COLUMN index;')
    
       


if __name__ == "__main__":
    rdfConverter = RDFtoPostgres()
    # rdfConverter.populateDatabase()
    rdfConverter.populateDatabaseFromPickle("LUBM_Database")
    # rdfConverter.parseDatalogTextFile("LUBM_DatalogProgram.txt")