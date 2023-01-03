import unittest
from DatalogReasoner import NaiveEval

class NaiveEvalTestSelecter(unittest.TestCase):

    def test_select_1(self):

        formula = "table_0(x,z) :- table_1(x,y), table_2(y,z)"
        statement = "INSERT INTO table_0 SELECT DISTINCT table_1_0.a, table_2_0.b FROM table_1 table_1_0, table_2 table_2_0 WHERE table_1_0.b = table_2_0.a ON CONFLICT DO NOTHING;"

        n = NaiveEval([formula], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == []

        assert n.testSelectQueryGeneration() == [statement]

    def test_select_2(self):

        formula = "table_0(x,z) :- table_1(x,y), table_2(y,z), table_3(y,z)"
        statement = "INSERT INTO table_0 SELECT DISTINCT table_1_0.a, table_2_0.b FROM table_1 table_1_0, table_2 table_2_0, table_3 table_3_0 WHERE table_1_0.b = table_2_0.a AND table_1_0.b = table_3_0.a AND table_2_0.b = table_3_0.b ON CONFLICT DO NOTHING;"

        n = NaiveEval([formula], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == []

        assert n.testSelectQueryGeneration() == [statement]
    
    def test_select_3(self):

        formula = "table_1(x,z) :- table_2(a,b)"

        n = NaiveEval([formula], True)

        with self.assertRaises(Exception):
            n.verificationTest()
        
    def test_4(self):

        formula = "table_1(x,z) :- table_1(x,y,z)"

        n = NaiveEval([formula], True)

        with self.assertRaises(Exception):
            n.verificationTest()

    def test_5(self):

        formula = "_a(x,y) :- table_1(x,y)"  #Have different attribute types

        n = NaiveEval([formula], True)

        with self.assertRaises(Exception):
            n.verificationTest()

    def test_6(self):

        formula = "table_1('a'%text, 2%integer) :- " 

        n = NaiveEval([formula], True)

        with self.assertRaises(Exception):
            n.verificationTest()

    def test_7(self):

        formula1 = "new_table(x) :- table_1(x,y)" 
        formula2 = "new_table(1%integer) :- "

        n = NaiveEval([formula1, formula2], True)

        with self.assertRaises(Exception):
            n.verificationTest()
        
    def test_8(self):

        formula = "table_1(x) :- "

        n = NaiveEval([formula], True)

        with self.assertRaises(Exception):
            n.verificationTest()

    def test_9(self):

        formula = "table_0('a'%text,'b'%text) :- "

        statement = "INSERT INTO table_0 SELECT DISTINCT 'a', 'b' ON CONFLICT DO NOTHING;"

        n = NaiveEval([formula], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == []

        assert n.testSelectQueryGeneration() == [statement]

    def test_10(self):

        formula = "table_0(x,y) :- table_1(x,y)"

        statement = "INSERT INTO table_0 SELECT DISTINCT table_1_0.a, table_1_0.b FROM table_1 table_1_0 ON CONFLICT DO NOTHING;"

        n = NaiveEval([formula], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == []
        
        assert n.testSelectQueryGeneration() == [statement]

    def test_11(self):

        formula = "table_0(x,y) :- table_1(x,y), table_2(x,'a'%text)"

        statement = "INSERT INTO table_0 SELECT DISTINCT table_1_0.a, table_1_0.b FROM table_1 table_1_0, table_2 table_2_0 WHERE table_1_0.a = table_2_0.a AND table_2_0.b = 'a' ON CONFLICT DO NOTHING;"

        n = NaiveEval([formula], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == []

        assert n.testSelectQueryGeneration() == [statement]

    def test_12(self):

        formula = "new_table(x) :- table_1(x,y)"

        statement = "INSERT INTO new_table SELECT DISTINCT table_1_0.a FROM table_1 table_1_0 ON CONFLICT DO NOTHING;" #do not need distinct
        createStatement = "CREATE TABLE IF NOT EXISTS new_table (_0 text, UNIQUE(_0));"
        n = NaiveEval([formula], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == [createStatement]
        
        assert n.testSelectQueryGeneration() == [statement]

    def test_13(self):

        formula = "new_table(2%integer) :- "

        statement = "INSERT INTO new_table SELECT DISTINCT 2 ON CONFLICT DO NOTHING;"
        createStatement = "CREATE TABLE IF NOT EXISTS new_table (_0 integer, UNIQUE(_0));"
        n = NaiveEval([formula], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == [createStatement]
        
        assert n.testSelectQueryGeneration() == [statement]

    def test_14(self):

        formula = "table_0(x,y) :- new_table(x,y)"

        statement = "INSERT INTO table_0 SELECT DISTINCT new_table_0._0, new_table_0._1 FROM new_table new_table_0 ON CONFLICT DO NOTHING;"
        createStatement = "CREATE TABLE IF NOT EXISTS new_table (_0 text, _1 text, UNIQUE(_0, _1));"
        n = NaiveEval([formula], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == [createStatement]
        
        assert n.testSelectQueryGeneration() == [statement]

    def test_15(self):

        formula1 = "table_0(x,y) :- new_table(x,y)"
        formula2 = "new_table(x,'a'%text) :- new_table_2(2%integer,x)"

        statement1 = "INSERT INTO table_0 SELECT DISTINCT new_table_0._0, new_table_0._1 FROM new_table new_table_0 ON CONFLICT DO NOTHING;"
        statement2 = "INSERT INTO new_table SELECT DISTINCT new_table_2_0._1, 'a' FROM new_table_2 new_table_2_0 WHERE new_table_2_0._0 = 2 ON CONFLICT DO NOTHING;"

        createStatement1 = "CREATE TABLE IF NOT EXISTS new_table (_0 text, _1 text, UNIQUE(_0, _1));"
        createStatement2 = "CREATE TABLE IF NOT EXISTS new_table_2 (_0 integer, _1 text, UNIQUE(_0, _1));"

        n = NaiveEval([formula1, formula2], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == [createStatement1, createStatement2]
        
        assert n.testSelectQueryGeneration() == [statement1, statement2]

    def test_16(self):

        formula = "_i(x) :- new_table(x), new_table(y), _i(y)"

        statement = "INSERT INTO _i SELECT DISTINCT new_table_0._0 FROM new_table new_table_0, new_table new_table_1, _i _i_0 WHERE new_table_1._0 = _i_0.a ON CONFLICT DO NOTHING;"
        createStatement = "CREATE TABLE IF NOT EXISTS new_table (_0 text, UNIQUE(_0));"
        n = NaiveEval([formula], True)
        try:
            n.verificationTest()
        except:
            raise Exception("Failed verification test")

        assert n.testCreateQueryGeneration() == [createStatement]
        
        assert n.testSelectQueryGeneration() == [statement]

if __name__ == '__main__':
    unittest.main()