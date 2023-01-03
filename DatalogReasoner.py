import psycopg2
import time

class NaiveEval:
    def __init__(self, program, testMode):
        self.program = self.parseProgram(program)
        conn = psycopg2.connect(
            database="Datalog_Sample", user='postgres', password='metamorphic', host='localhost', port= '5433'
        )

        self.cursor = conn.cursor()
        conn.autocommit = True
        self.relationDict = self.populateRelationDictionary()
        self.databasePredicates = self.relationDict.copy()
        if(testMode == False):
            self.verifyPredicateSizes()
            self.verifyVariables()
            self.adjList = self.createGraph()
            self.verifyTypes(self.findConnectedComponents())
            print(self.relationDict)
            self.generateCreateQueries()
            self.addUniqueConstraints()
            #time here
            start = time.perf_counter()
            self.generateSelectQueries()
            end = time.perf_counter()
            print(f"Time taken: {end-start}")
            conn.close()

    def parseProgram(self, program):
        parsedProgram = []

        for formula in program:
            
            parsedProgram.append(self.parseFormula(formula))
        
        return parsedProgram

    def parseFormula(self, formula):
        formula = formula.split(" :- ")
        head = formula[0]
        body = formula[1]
        body = body.split(", ")
        
        if(body == [""]):
            body = []

        for i in range(0, len(body)):
            predicate = body[i]
            newPredicate = ()
            split = predicate.split("(")
            predHead = split[0]
            split[1] = split[1][0:len(split[1]) - 1]
            predBody = split[1].split(",")
            newPredicate = newPredicate + (predHead,)
            for bodyElement in predBody:
                if "%" in bodyElement:
                    bodyElement = (bodyElement.split("%")[0], bodyElement.split("%")[1])
                newPredicate = newPredicate + (bodyElement,)
            body[i] = newPredicate

        newPredicate = ()
        split = head.split("(")
        split[1] = split[1][0:len(split[1]) - 1]
        predBody = split[1].split(",")
        newPredicate = newPredicate + (split[0],)
        for bodyElement in predBody:
            if "%" in bodyElement:
                bodyElement = (bodyElement.split("%")[0], bodyElement.split("%")[1])
            newPredicate = newPredicate + (bodyElement,)
        head = newPredicate

        return [head] + body


    def populateRelationDictionary(self):
        relationDict = {}
        self.cursor.execute("""SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'""")

        tables = self.cursor.fetchall()

        for table in tables:
            self.cursor.execute(
                f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table[0]}' ORDER BY ordinal_position;"
            )
            columnList = self.cursor.fetchall()
            relationDict.update({table[0]: columnList})

        return relationDict


    def verifyPredicateSizes(self):
        for formula in self.program:
            for atom in formula:
                attributes = self.relationDict.get(atom[0])
                if(attributes is not None):
                    assert len(attributes) == len(atom) - 1
                else:
                    self.relationDict.update({atom[0]: [("_" + str(i), "") for i in range(0, len(atom) - 1)]})

    
    def createGraph(self):
        adjList = {}
        for pred in self.relationDict.keys():
            attributes = self.relationDict.get(pred)
            for i in range(0, len(attributes)):
                adjList.update({pred + "." + str(i): []})

        for formula in self.program:
            localVarDict = {}
            for atom in formula:
                for i in range(1, len(atom)):
                    if(atom[i] not in localVarDict.keys() and isinstance(atom[i], str)):
                        localVarDict.update({atom[i]: atom[0] + "." + str(i-1)})
                    elif(isinstance(atom[i], str)):
                        adjList.get(localVarDict.get(atom[i])).append(atom[0] + "." + str(i-1))
                        adjList.get(atom[0] + "." + str(i-1)).append(localVarDict.get(atom[i]))

                    else: #Update constant types
                        attributes = self.relationDict.get(atom[0])
                        if(attributes[i-1][1] == ""):
                            attributes[i-1] = (attributes[i-1][0], atom[i][1])
                        else:
                            assert attributes[i-1][1] == atom[i][1]
        
        return adjList


    def findConnectedComponents(self):
        visited = {}
        for vertex in self.adjList.keys():
            visited.update({vertex: False})
        
        connectedComponents = []
        for vertex in self.adjList.keys():
            if(not visited.get(vertex)):
                temp = []
                connectedComponents.append(self.DFS(temp, vertex, visited))
        
        return connectedComponents
    
    def DFS(self, temp, vertex, visited):
        visited[vertex] = True
        temp.append(vertex)

        for u in self.adjList.get(vertex):
            if(not visited.get(u)):
                temp = (self.DFS(temp, u, visited))

        return temp
    

    def verifyTypes(self, cc):
        for component in cc:
            attr_type = ""
            for attribute in component:
                attribute = attribute.split(".")
                pred = attribute[0]
                attr_pos = int(attribute[1])
                
                dict_attrs = self.relationDict.get(pred)
                if(dict_attrs[attr_pos][1] != ""):
                    if(attr_type != ""):
                        assert attr_type == dict_attrs[attr_pos][1]
                    else:
                        attr_type = dict_attrs[attr_pos][1]
            assert attr_type != ""
            for attribute in component:
                attribute = attribute.split(".")
                pred = attribute[0]
                attr_pos = int(attribute[1])
                dict_attrs = self.relationDict.get(pred)
                dict_attrs[attr_pos] = (dict_attrs[attr_pos][0], attr_type)

    def verifyVariables(self):
        for formula in self.program:
            localVarDict = {}

            head = formula[0]
            body = formula[1:len(formula)]

            for atom in body:
                for i in range(1, len(atom)):
                    if(isinstance(atom[i], str)):
                        localVarDict.update({atom[i]: True})
            for i in range(1, len(head)):
                if(isinstance(head[i], str)):
                    assert localVarDict.get(head[i]) is not None


    def verificationTest(self):
        self.verifyPredicateSizes()
        self.verifyVariables()
        self.adjList = self.createGraph()
        self.verifyTypes(self.findConnectedComponents())
            
    
    def generateCreateQueries(self):
        for relation in self.relationDict.keys():
            if(self.databasePredicates.get(relation) is None):
                createStmt = f"CREATE TABLE IF NOT EXISTS {relation} ("
                attributes = self.relationDict.get(relation)
                for attribute in attributes:
                    createStmt += f"{attribute[0]} {attribute[1]}, "
                createStmt += "UNIQUE("
                for attribute in attributes:
                    createStmt += f"{attribute[0]}, "
                createStmt = createStmt[0:len(createStmt) - 2] + "));"   
                print(createStmt)
                self.cursor.execute(createStmt)

    def testCreateQueryGeneration(self):
        createStmts = []
        for relation in self.relationDict.keys():
            if(self.databasePredicates.get(relation) is None):
                createStmt = f"CREATE TABLE IF NOT EXISTS {relation} ("
                attributes = self.relationDict.get(relation)
                for attribute in attributes:
                    createStmt += f"{attribute[0]} {attribute[1]}, "
                createStmt += "UNIQUE("
                for attribute in attributes:
                    createStmt += f"{attribute[0]}, "
                createStmt = createStmt[0:len(createStmt) - 2] + "));"   
                createStmts.append(createStmt)
        return createStmts
                
    
    def addUniqueConstraints(self):
        for relation in self.databasePredicates.keys():
            constraint_name = relation + "_unique"

            #First drop if exists:
            alterStmt = f"ALTER TABLE {relation} DROP CONSTRAINT IF EXISTS {constraint_name};"
            self.cursor.execute(alterStmt)
            #RUN QUERY

            alterStmt = f"ALTER TABLE {relation} ADD CONSTRAINT {constraint_name} UNIQUE("
            attributes = self.databasePredicates.get(relation)
            for attribute in attributes:
                alterStmt += f"{attribute[0]}, "
            alterStmt = alterStmt[0:len(alterStmt) - 2] + ");"   
            self.cursor.execute(alterStmt)
            #RUN ALTER QUERIES

    
    def generateSelectQueries(self):
        maxInsert = 0
        for formula in self.program:

            selectStmt = self.generateSelectQueryWithAliases(formula)
            
            #RUN SELECT QUERY - you should change the above to store the select queries in main memory/disk
            maxInsert += self.insert_with_rowcount(selectStmt)
        if(maxInsert == 0):
            print("Naive Evaluation Complete")
        else:
            self.generateSelectQueries()
    
    def testSelectQueryGeneration(self):
        selectStmts = []

        for formula in self.program:
            selectStmt = self.generateSelectQueryWithAliases(formula)
            selectStmts.append(selectStmt)

        return selectStmts

    def generateSelectQuery(self, formula):
        head = formula[0]
        body = formula[1:len(formula)]
        selectStmt = f"INSERT INTO {head[0]} SELECT DISTINCT " #Should we be using distinct?
        for i in range(1, len(head)):
            if(isinstance(head[i], str)):
                for atom in body:
                    breakFlag = False
                    for j in range(1, len(atom)):
                        if(head[i] == atom[j]):
                            selectStmt += f"{atom[0]}.{self.relationDict.get(atom[0])[j-1][0]}, "
                            breakFlag = True                
                            break
                    if(breakFlag):
                        break
            else: #Constant !!!
                selectStmt += f"{head[i][0]}, "
        selectStmt = selectStmt[0: len(selectStmt) - 2]

        if(len(body) > 0):
            selectStmt += " FROM "

            for atom in body:
                selectStmt += f"{atom[0]}, "
            selectStmt = selectStmt[0: len(selectStmt) - 2] + " WHERE "
            whereFlag = False
            localVarDict = {}
            for atom in body:
                for i in range(1, len(atom)):
                    if(atom[i] not in localVarDict.keys() and isinstance(atom[i], str)):
                        localVarDict.update({atom[i]: f"{atom[0]}.{self.relationDict.get(atom[0])[i-1][0]}"})
                    elif(isinstance(atom[i], str)):
                        selectStmt += f"{localVarDict.get(atom[i])} = "
                        selectStmt += f"{atom[0]}.{self.relationDict.get(atom[0])[i-1][0]} AND "
                        whereFlag = True
                    else: #Constant
                        selectStmt += f"{atom[0]}.{self.relationDict.get(atom[0])[i-1][0]} = {atom[i][0]} AND "
                        whereFlag = True
            
            if(whereFlag):
                selectStmt = selectStmt[0: len(selectStmt) - 5]
            else:
                selectStmt = selectStmt[0: len(selectStmt) - 7]
        
        selectStmt += " ON CONFLICT DO NOTHING;"
        # print(selectStmt)
        return selectStmt

    def generateSelectQueryWithAliases(self, formula):
        head = formula[0]
        body = formula[1:len(formula)]
        selectStmt = f"INSERT INTO {head[0]} SELECT DISTINCT " #Should we be using distinct?

        aliasDict = {}
        for i in range(0, len(body)):
            if(body[i][0] not in aliasDict.keys()):
                aliasDict.update({body[i][0]: 0})
                body[i] = (0,) + body[i]
            else:
                num = aliasDict.get(body[i][0])
                aliasDict.update({body[i][0]: num+1})
                body[i] = (num+1,) + body[i]
                
        for i in range(1, len(head)):
            if(isinstance(head[i], str)):
                for atom in body:
                    breakFlag = False
                    for j in range(2, len(atom)):
                        if(head[i] == atom[j]):
                            selectStmt += f"{atom[1]}_{atom[0]}.{self.relationDict.get(atom[1])[j-2][0]}, "
                            breakFlag = True                
                            break
                    if(breakFlag):
                        break
            else: #Constant !!!
                selectStmt += f"{head[i][0]}, "
        selectStmt = selectStmt[0: len(selectStmt) - 2]

        if(len(body) > 0):
            selectStmt += " FROM "

            for atom in body:
                selectStmt += f"{atom[1]} {atom[1]}_{atom[0]}, "
            selectStmt = selectStmt[0: len(selectStmt) - 2] + " WHERE "
            whereFlag = False
            localVarDict = {}
            for atom in body:
                for i in range(2, len(atom)):
                    if(atom[i] not in localVarDict.keys() and isinstance(atom[i], str)):
                        localVarDict.update({atom[i]: f"{atom[1]}_{atom[0]}.{self.relationDict.get(atom[1])[i-2][0]}"})
                    elif(isinstance(atom[i], str)):
                        selectStmt += f"{localVarDict.get(atom[i])} = "
                        selectStmt += f"{atom[1]}_{atom[0]}.{self.relationDict.get(atom[1])[i-2][0]} AND "
                        whereFlag = True
                    else: #Constant
                        selectStmt += f"{atom[1]}_{atom[0]}.{self.relationDict.get(atom[1])[i-2][0]} = {atom[i][0]} AND "
                        whereFlag = True
            
            if(whereFlag):
                selectStmt = selectStmt[0: len(selectStmt) - 5]
            else:
                selectStmt = selectStmt[0: len(selectStmt) - 7]
        
        selectStmt += " ON CONFLICT DO NOTHING;"
        # print(selectStmt)
        return selectStmt

    def insert_with_rowcount(self, statement):
        self.cursor.execute(statement)
        rowcount = self.cursor.rowcount
        # self.conn.commit()
        return rowcount



if __name__ == "__main__":
    datalogFormula1 = "_c(x,y) :- _a(x,z), _b(z,y)"
    datalogFormula2 = "_e('1'%character varying,2%integer) :- "
    datalogFormula3 = "_f(x,y) :- _e(y,x)"
    datalogFormula4 = "_h(y,z) :- table_0(x,y), table_1(x,z)"
    n = NaiveEval([datalogFormula4], False)



