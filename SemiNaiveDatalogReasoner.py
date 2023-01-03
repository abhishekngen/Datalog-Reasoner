import psycopg2
import time
import copy

class SemiNaiveEval:
    def __init__(self, program, testMode):
        self.program = self.parseProgram(program)
        self.parseSemiNaiveProgram()
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

    def parseSemiNaiveProgram(self):
        self.semiNaiveProgram = []

        for formula in self.program:
            head = formula[0]
            body = formula[1:len(formula)]
            seminaiveBody = copy.deepcopy(body)
            if(len(seminaiveBody) > 0):
                for i in range(0, len(seminaiveBody)):
                    for j in range(0, len(seminaiveBody)-i-1):
                        seminaiveBody[j] = ["old", seminaiveBody[j]]
                    seminaiveBody[len(seminaiveBody)-i-1] = ["delta_old", seminaiveBody[len(seminaiveBody)-i-1]]

                    for j in range(len(seminaiveBody)-i, len(seminaiveBody)):
                        seminaiveBody[j] = ["actual", seminaiveBody[j]]

                    self.semiNaiveProgram.append([head] + seminaiveBody)

                    seminaiveBody = copy.deepcopy(body)
            else:
                self.semiNaiveProgram.append([head])
        print(self.semiNaiveProgram)


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
                    print("yee")
                    print(attributes)
                    print(atom)
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

            #Append seminaive columns:
        for relation in self.relationDict.keys():
            alterStmt = f"""
            ALTER TABLE {relation}
            ADD COLUMN IF NOT EXISTS flag INTEGER DEFAULT 2;
            """

            self.cursor.execute(alterStmt)

    def generateSelectQueries(self):

        selectStmts = []
        for formula in self.semiNaiveProgram:

            selectStmts.append(self.generateSelectQueryWithAliases(formula))
        
        self.executeSelectQueries(selectStmts)

    def executeSelectQueries(self, selectStmts):
        maxInsert = 0
        for stmt in selectStmts:
            maxInsert += self.insert_with_rowcount(stmt)
        if(maxInsert == 0):
            print("Seminaive Evaluation Complete")
            self.dropFlagColumn()
        else:
            self.updateRelations()
            self.executeSelectQueries(selectStmts)

    def generateSelectQueryWithAliases(self, formula):
        head = formula[0]
        body = formula[1:len(formula)]
        selectStmt = f"INSERT INTO {head[0]} SELECT " #Should we be using distinct? - answer is no lol
        whereStmt = " WHERE "
        aliasDict = {}
        for i in range(0, len(body)):
            if(body[i][1][0] not in aliasDict.keys()):
                aliasDict.update({body[i][1][0]: 0})
                print("yeet")
                print(body[i][1])
                body[i][1] = (0,) + body[i][1]
            else:
                num = aliasDict.get(body[i][1][0])
                aliasDict.update({body[i][1][0]: num+1})
                body[i][1] = (num+1,) + body[i][1]
                
        for i in range(1, len(head)):
            if(isinstance(head[i], str)):
                for atom in body:
                    breakFlag = False
                    for j in range(2, len(atom[1])):
                        if(head[i] == atom[1][j]):
                            selectStmt += f"{atom[1][1]}_{atom[1][0]}.{self.relationDict.get(atom[1][1])[j-2][0]}, "
                            breakFlag = True                
                            break
                    if(breakFlag):
                        break
            else: #Constant !!!
                selectStmt += f"{head[i][0]}, "
        selectStmt += "3"

        if(len(body) > 0):
            selectStmt += " FROM "

            for atom in body:
                selectStmt += f"{atom[1][1]} {atom[1][1]}_{atom[1][0]}, "
            selectStmt = selectStmt[0: len(selectStmt) - 2] + whereStmt
            localVarDict = {}
            for atom in body:
                for i in range(2, len(atom[1])):
                    if(atom[1][i] not in localVarDict.keys() and isinstance(atom[1][i], str)):
                        localVarDict.update({atom[1][i]: f"{atom[1][1]}_{atom[1][0]}.{self.relationDict.get(atom[1][1])[i-2][0]}"})
                    elif(isinstance(atom[1][i], str)):
                        selectStmt += f"{localVarDict.get(atom[1][i])} = "
                        selectStmt += f"{atom[1][1]}_{atom[1][0]}.{self.relationDict.get(atom[1][1])[i-2][0]} AND "
                    else: #Constant
                        selectStmt += f"{atom[1][1]}_{atom[1][0]}.{self.relationDict.get(atom[1][1])[i-2][0]} = {atom[1][i][0]} AND "
         
            for atom in body:
                if(atom[0] == "old"):
                    selectStmt += f"{atom[1][1]}_{atom[1][0]}.flag = 1 AND "
                elif(atom[0] == "delta_old"):
                    selectStmt += f"{atom[1][1]}_{atom[1][0]}.flag = 2 AND "
                else:
                    selectStmt += f"{atom[1][1]}_{atom[1][0]}.flag <= 2 AND "

            selectStmt = selectStmt[0: len(selectStmt) - 5]
        
        selectStmt += " ON CONFLICT DO NOTHING;"
        print(selectStmt)
        return selectStmt

    def testSelectQueryGeneration(self):
        selectStmts = []

        for formula in self.semiNaiveProgram:
            selectStmt = self.generateSelectQueryWithAliases(formula)
            selectStmts.append(selectStmt)

        return selectStmts

    def updateRelations(self):
        for relation in self.relationDict.keys():
            updateStmt = f"""
            UPDATE {relation}
            SET flag = flag - 1
            WHERE flag >= 2;
            """
            self.cursor.execute(updateStmt)

    def dropFlagColumn(self):
        for relation in self.relationDict.keys():
            alterStmt = f"""
            ALTER TABLE {relation}
            DROP COLUMN IF EXISTS flag;
            """
            self.cursor.execute(alterStmt)

    def insert_with_rowcount(self, statement):
        self.cursor.execute(statement)
        rowcount = self.cursor.rowcount
        # self.conn.commit()
        return rowcount
    
datalogFormula1 = "s1(x) :- s2(x,y), s3(y)"
datalogFormula2 = "s3('b'%text) :- "
n = SemiNaiveEval([datalogFormula1, datalogFormula2], False)
