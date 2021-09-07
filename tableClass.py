import logging
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

class TXTCassandra:

    def __init__(self):
        self.cluster = None
        self.keyspace = None
        self.gram = None
        self.predict = None

    def __del__(self):
        self.cluster.shutdown()

    def createsession(self):
        self.cluster = Cluster(['localhost'])
        self.session = self.cluster.connect(self.keyspace)

    def getsession(self):
        return self.session

    def createkeyspace(self, keyspace):
        rows = self.session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
        if keyspace in [row[0] for row in rows]:
            self.session.execute("DROP KEYSPACE " + keyspace)
        self.session.execute("""CREATE KEYSPACE %s
                        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
                        """ % keyspace)
        self.session.set_keyspace(keyspace)

    def create_table(self, gram):
        c_sql = 'CREATE TABLE IF NOT EXISTS ' + str(gram) + ' (word varchar PRIMARY KEY, count int);'
        self.gram = gram
        self.session.execute(c_sql)

    def create_table_next(self, gram):
        c_sql = 'CREATE TABLE IF NOT EXISTS ' + str(
            gram) + ' (firstWord varchar PRIMARY KEY, listWord list<tuple<varchar, int>>);'
        self.gram = gram
        self.session.execute(c_sql)

    def insert_data_next(self, fwl, ll):
        insert_sql = self.session.prepare("INSERT INTO " + str(self.gram) + "(firstWord, listWord) VALUES (?, ?)")
        for x in range(len(fwl)):
            self.session.execute(insert_sql.bind((fwl[x], ll[x])))

    def insert_data_alt(self, wl, cl):
        insert_sql = self.session.prepare("INSERT INTO " + str(self.gram) + "(word, count) VALUES (?, ?)")
        for x in range(len(wl)):
            self.session.execute(insert_sql.bind((wl[x], int(cl[x]))))

    def select_data(self):
        rows = self.session.execute('select * from ' + str(self.gram) + " limit 55000;")
        return rows

    def select_specific_data(self, name, wl):
        if name in wl == False:
            print("DNE in table")
            return None
        rows = self.session.execute("SELECT listword from " + str(self.gram) + " WHERE firstWord = '" + name + "'")
        #for row in rows:
        #    print(row.count)
        return rows.one().listword

    def update_data_next(self, firstWord, nextWord, count):
        update_tuple = (nextWord, count)
        tmp = dict(self.select_data())
        if tmp.get(firstWord) == None:
            tmp_string = "INSERT INTO {0} (firstWord, listWord) VALUES (?, ?) IF NOT EXISTS;".format(str(self.gram))
            insert_sql = self.session.prepare(tmp_string)
            self.session.execute(insert_sql.bind((firstWord, [update_tuple])))
            # self.insert_data_next([firstWord], [(count, nextWord)])
        else:
            tmp_row = tmp.get(firstWord)
            exists = False
            for x in range(len(tmp_row)):
                if tmp_row[x][0] == nextWord:
                    exists = True
                    tempStruct = list(tmp_row[x])
                    tempStruct[1] += count
                    tmp_row[x] = tuple(tempStruct)
                    break
            if exists == False:
                tmp_row.append(update_tuple)
            tmp_row.sort(key=lambda x: x[1], reverse=True)
            tmp_string = "UPDATE {0} SET listWord={1} WHERE firstWord='{2}';".format(str(self.gram), tmp_row,
                                                                                     str(firstWord))
            update_sql = self.session.prepare(tmp_string)
            self.session.execute(update_sql)

