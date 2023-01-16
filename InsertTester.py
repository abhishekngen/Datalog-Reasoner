from SemiNaiveDatalogReasoner import SemiNaiveEval
from RDFtoPostgres import RDFtoPostgres

rdfPopulator = RDFtoPostgres()
rdfPopulator.populateDatabaseFromPickle("LUBM_Database")

print("Running NOT EXISTS Insert Method:")
seminaive1 = SemiNaiveEval("LUBM_DatalogProgram.txt", True, False)

rdfPopulator.populateDatabaseFromPickle("LUBM_Database")

print("Running UNIQUE Insert Method:")
seminaive1 = SemiNaiveEval("LUBM_DatalogProgram.txt", False, False)