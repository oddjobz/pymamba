#!/usr/bin/python3
##############################################################################
#
# MIT License
#
# Copyright (c) 2017 Gareth Bult
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
##############################################################################
#
#   This is the current implementation of "mamba" which is a database layout
#   that sites directly on top of LMDB. Currently it's *much* faster than
#   Mongo, but currently incomplete and untested .. but it's great for 
#   playing with.
#
##############################################################################
from lmdb import Cursor, Environment
from ujson import loads, dumps
from sys import _getframe
from uuid import uuid1 as UUID
from subprocess import call
import unittest

##############################################################################
#   utility routines
##############################################################################
def debug(self, msg):
    """debug routine"""
    if not self._debug: return
    line = _getframe(1).f_lineno
    name = _getframe(1).f_code.co_name
    print("{}: #{} - {}".format(name, line, msg))

def anonymous(text):
    scope = {}
    exec("def func" + text, scope)
    return scope['func']

##############################################################################
#   CLASS - Exceptions
##############################################################################
class lmdb_TableExists(Exception):
    pass

class lmdb_IndexExists(Exception):
    pass

class lmdb_TableMissing(Exception):
    pass

class lmdb_IndexMissing(Exception):
    pass

class lmdb_NotFound(Exception):
    pass
##############################################################################
#   CLASS - Index
##############################################################################
class Index(object):

    _debug = False
    _str_tpl = '(k): return str(k["{}"]).encode()'
    _int_tpl = '(k): return k["{}"].to_bytes(8,"big",signed=False)'

    def __init__(self, env, name, func, conf):
        self._env = env
        self._name = name
        self._conf = conf
        self._conf['key'] = self._conf['key'].encode()
        self._integer = conf.get('integerkey', False)
        if func[0] == '!':
            self._func = anonymous(func[1:])
        else:
            fmt = self._int_tpl if self._integer else self._str_tpl
            self._func = anonymous(fmt.format(func))
        self._db = self._env.open_db(**self._conf)

    def reindex(self):
        """reindex this index"""
        pass

    def drop(self, txn):
        """drop a database index"""
        return txn.drop(self._db, delete=False)

    def put(self, txn, key, record):
        """put a new record in the index"""
        return txn.put(self._func(record), key.encode(), db=self._db)

    def delete(self, txn, key, record):
        """delete a record from the index"""
        return txn.delete(self._func(record), key, db=self._db)

    def get(self, txn, record):
        """read a record based on the primary key"""
        return txn.get(self._func(record), db=self._db)

    def cursor(self, txn):
        """return a cursor into the index"""
        return Cursor(self._db, txn)

    def count(self, txn):
        """return the number of items present in the index"""
        return txn.stat(self._db).get('entries', 0)

##############################################################################
#   CLASS - Table
##############################################################################
class Table(object):

    _debug = False
    _indexes = {}

    def __init__(self, env, name=None):
        self._env = env
        self._name = name
        self._indexes = {}
        self._db = self._env.open_db(name.encode())
        for index in self.indexes:
            key = ''.join(['@', self._index_name(index)]).encode()
            with self._env.begin() as txn:
                doc = loads(bytes(txn.get(key)))
                self._indexes[index] = Index(self._env, index, doc['func'], doc['conf'])

    def _index_name(self, name):
        return '_{}_{}'.format(self._name, name)

    def count_index(self, name):
        index = self._indexes[name]
        with self._env.begin() as txn:
            return index.count(txn)

    def create_index(self, name, func, duplicates=False, integer=False):
        conf = {
            'key': self._index_name(name),
            'integerkey': integer,
            'integerdup': duplicates,
            'dupsort': duplicates,
            'create': True,
        }
        self._indexes[name] = Index(self._env, name, func, conf)
        with self._env.begin(write=True) as txn:
            key = ''.join(['@', self._index_name(name)]).encode()
            val = dumps({'conf': conf, 'func': func}).encode()
            txn.put(key, val)
            self._indexes[name].reindex()

    def delete_index(self, name):
        db = self._env.open_db(self._index_name(name).encode())
        with self._env.begin(write=True) as txn:
            txn.drop(db, True)
            txn.delete(''.join(['@', self._index_name(name)]).encode())

    def append(self, doc):
        key = str(UUID())
        doc['_id'] = key
        with self._env.begin(write=True) as txn:
            txn.put(key.encode(), dumps(doc).encode(), db=self._db, append=True)
            for name in self._indexes:
                self._indexes[name].put(txn, key, doc)

    def delete(self, ids):
        with self._env.begin(write=True) as txn:
            for _id in ids:
                key = _id.encode()
                doc = loads(bytes(txn.get(key, db=self._db)))
                txn.delete(key, db=self._db)
                for name in self._indexes:
                    self._indexes[name].delete(txn, key, doc)

    #def search(self, spec):
    #    results = []
    #    with self._env.begin() as txn:
    #        for idx, key in spec.items():
    #            if idx == '_id':
    #                results.append(key)
    #                break
    #            if idx not in self._indexes: raise lmdb_IndexMissing(idx)
    #            with self._indexes[idx].cursor(txn) as cursor:
    #                if cursor.set_range(index_name.encode()):
    #                    while True:
    #                        name = cursor.key().decode()
    #                        if not name.startswith(index_name) or not cursor.next():
    #                            break
    #                        results.append(name[pos:])


    #        print(idx,key)
    #    return {}

    def find(self, index_name=None, max=None):
        """
        find - public method to return records from this table
        """
        results = []
        with self._env.begin() as txn:
            if not index_name:
                with Cursor(self._db, txn) as cursor:
                    if not cursor.first(): return
                    count = 0
                    while True:
                        doc = cursor.value()
                        results.append(loads(bytes(doc)))
                        count += 1
                        if not cursor.next() or (max and count>=max): break
            else:
                if index_name not in self._indexes:
                    raise lmdb_IndexMissing(index_name)
                index = self._indexes[index_name]
                with index.cursor(txn) as cursor:
                    if not cursor.first(): return
                    count = 0
                    while True:
                        doc = cursor.value()
                        doc = loads(bytes(txn.get(doc, db=self._db)))
                        results.append(doc)
                        count += 1
                        if not cursor.next() or (max and count>=max): break

        return results

    @property
    def indexes(self):
        """
        indexes - return a list of index names for this table
        """
        results = []
        index_name = self._index_name('')
        pos = len(index_name)
        with self._env.begin() as txn:
            db = self._env.open_db()
            with Cursor(db, txn) as cursor:
                if cursor.set_range(index_name.encode()):
                    while True:
                        name = cursor.key().decode()
                        if not name.startswith(index_name) or not cursor.next():
                            break
                        results.append(name[pos:])
        return results

    @property
    def count(self):
        with self._env.begin() as txn:
            return txn.stat(self._db).get('entries', 0)

##############################################################################
#   CLASS - Database
##############################################################################
class Database(object):

    _config = {
        'map_size': 1024*1024*1024 * 2,
        'subdir': True,
        'metasync': False,
        'sync': True,
        'max_dbs': 256,
        'writemap': True
    }
    def __init__(self, *args, **argv):
        _config = dict(self._config, **argv.get('env', {}))
        self._tables = {}
        self._env = Environment(args[0], **_config)
        self._db = self._env.open_db()

    def __del__(self):
        self.close()

    def close(self):
        """
        close - public method to close the database
        """
        if not self._env: return
        self._env.close()
        self._env = None

    def clear(self, name):
        """
        clear - public method to clear the contents of a table
        """
        return self.drop(name, False)

    def drop(self, name, delete=True):
        """
        drop - public method to drop a table
        """
        if not self.table_exists(name): raise lmdb_TableMissing(name)
        table = self.table(name)
        for index in table.indexes:
            table.delete_index(index)
        db = self._env.open_db(name.encode())
        with self._env.begin(write=True) as txn:
            txn.drop(db, delete)
        return True

    def table(self, name):
        """
        table - public method to get a handle for a table
        """
        if name not in self._tables:
            self._tables[name] = Table(self._env, name)
        return self._tables[name]

    def table_exists(self, table_name):
        return table_name in self.tables

    def index_exists(self, table_name, index_name):
        key = '_{}_{}'.format(table_name, index_name)
        return key in self.tables

    def create_index(self, table_name, index_name, func, duplicates=False, integer=True):
        """
        create_index - public method to create a new index
        """
        if not self.table_exists(table_name):             raise lmdb_TableMissing(table_name)
        if self.index_exists(table_name, index_name):     raise lmdb_IndexExists(index_name)
        return self.table(table_name).create_index(index_name, func, duplicates, integer)

    def delete_index(self, table_name, index_name):
        """
        delete_index - public method to delete an index
        """
        if not self.table_exists(table_name):             raise lmdb_TableMissing(table_name)
        if not self.index_exists(table_name, index_name): raise lmdb_IndexMissing(index_name)
        return self.table(table_name).delete_index(index_name)

    def indexes(self, name):
        """
        indexes - public method to get the indexes for a table
        """
        return self.table(name).indexes

    @property
    def tables(self):
        """
        tables - public property to list tables in the database
        """
        result = []
        with self._env.begin() as txn:
            with Cursor(self._db, txn) as cursor:
                if cursor.first():
                    while True:
                        name = cursor.key().decode()
                        result.append(name)
                        if not cursor.next():
                            break
        return result


class UnitTests(unittest.TestCase):

    _database = 'unit-db'

    def setUp(self):
        call(['rm', '-rf', self._database])

    def tearDown(self):
        pass

    def test_01_basic(self):
        db = Database(self._database)
        self.assertEqual(db.tables, [])

    def test_02_create_drop(self):
        db = Database(self._database)
        self.assertEqual(db.tables, [])
        db.table('demo1')
        self.assertEqual(db.tables, ['demo1'])
        db.drop('demo1')
        self.assertEqual(db.tables, [])

    def test_03_create_drop_index(self):
        db = Database(self._database)
        db.table('demo1')
        db.create_index('demo1','by_name', 'name')
        db.create_index('demo1','by_age', 'age', integer=True, duplicates=True)
        self.assertEqual(db.indexes('demo1'), ['by_age', 'by_name'])
        db.drop('demo1')
        self.assertEqual(db.tables, [])

    def test_04_put_data(self):

        data = [
            {'name': 'Gareth Bult', 'age': 21},
            {'name': 'Squizzey', 'age': 3000},
            {'name': 'Fred Bloggs', 'age': 45},
            {'name': 'John Doe', 'age': 0},
            {'name': 'John Smith', 'age': 40},
            {'name': 'Gareth Bult1', 'age': 21}
        ]
        people = {}

        db = Database(self._database)
        table = db.table('demo1')
        table.create_index('by_name', 'name')
        table.create_index('by_age', 'age', integer=True, duplicates=True)
        for item in data:
            table.append(item)
            people[item['name']] = item
        results = table.find()
        for item in results:
            key = item.get('name', None)
            self.assertIsNotNone(key)
            if key:
                person = people.get(key, None)
                self.assertIsNotNone(person)
                if person:
                    self.assertEqual(person['age'], item['age'])
                    self.assertEqual(person['_id'], item['_id'])

        results = table.find('by_name')
        last = ''
        for item in results:
            key = item.get('name', None)
            self.assertIsNotNone(key)
            if key:
                person = people.get(key, None)
                self.assertIsNotNone(person)
                if person:
                    self.assertEqual(person['age'], item['age'])
                    self.assertEqual(person['_id'], item['_id'])
                    self.assertGreaterEqual(person['name'], last)
                    last = person['name']

        results = table.find('by_age')
        last = 0
        for item in results:
            key = item.get('name', None)
            self.assertIsNotNone(key)
            if key:
                person = people.get(key, None)
                self.assertIsNotNone(person)
                if person:
                    self.assertEqual(person['age'], item['age'])
                    self.assertEqual(person['_id'], item['_id'])
                    self.assertGreaterEqual(person['age'], last)
                    last = person['age']

        self.assertEqual(table.count,len(data))
        self.assertEqual(table.count_index('by_name'),len(data))
        self.assertEqual(table.count_index('by_age'),len(data))

        table.delete([results[0]['_id']])
        table.delete([results[1]['_id']])
        table.delete([results[2]['_id']])

        self.assertEqual(table.count,len(data)-3)
        self.assertEqual(table.count_index('by_name'),len(data)-3)
        self.assertEqual(table.count_index('by_age'),len(data)-3)

        results = table.find('by_age')
        last = 0
        for item in results:
            print(item)
            key = item.get('name', None)
            self.assertIsNotNone(key)
            if key:
                person = people.get(key, None)
                self.assertIsNotNone(person)
                if person:
                    self.assertEqual(person['age'], item['age'])
                    self.assertEqual(person['_id'], item['_id'])
                    self.assertGreaterEqual(person['age'], last)
                    last = person['age']

        db.drop('demo1')
        self.assertEqual(db.tables, [])


if __name__ == "__main__":
    unittest.main()

    
    
    #print(">", db.tables)
    #db.create_index('demo1','by_name', 'name')
    #db.create_index('demo1','by_age', 'age')
    #list(db)
    #db.delete_index('demo1', 'by_name')
    #list(db)
    #db.delete_index('demo1', 'by_age')
    #list(db)

#
 #   origins = db.table('origins')
  #  origins.iterate()


    #print("Create Index>",db.create_index('origins', 'by_origin', 'origin'))
#    origins.create_index('by_origin', 'origin')

#    sessions = db.table('sessions')
#    sessions.iterate(1)


##############################################################################
#
#   CLASS :: Table
#
##############################################################################
#class Table(object):

#    _debug = False
#    _index_array = {}

#    def __init__(self, env, name=None, indexes=None):
#        """set up access to a table object"""
#        self._env = env
#        self._name = name
#        self._indexes = indexes
#        self._init_autoinc()
#        self._init_indexes()

#    def _init_autoinc(self):
#        """set the autoinc sequence # to the last id on file"""
#        self._db = self._env.open_db(self._name.encode('ascii'))

#    def _init_indexes(self):
#        """set up the indexes relating to this table"""
#        for name, index in self._indexes.items():
#            debug(self, '++ {}'.format(index))
#            self._index_array[name] = Index(self._env, index)

#    def open_index(self, index):
#        self._index_array[index['name']] = Index(self._env, index)

#    def get(self, key, txn=None):
#        """recover a record using the primary key"""
#        def get_actual():
#            record = txn.get(key.encode(), db=self._db)
#            if not record: return None
#            return loads(bytes(record))
#        if txn: return get_actual()
#        with self._env.begin() as txn: return get_actual()

#    def drop(self, txn=None):
#        """drop all records from the table - USE WITH CARE!"""
#        def drop_actual():
#            txn.drop(self._db, delete=False)
#            for _, index in list(self._index_array.items()):
#                index.drop(txn)
#        if txn: drop_actual()
#        with self._env.begin(write=True) as txn: drop_actual()

#    def put(self, record, txn=None):
#        """put a new record into the table"""
#        def put_actual():
#            key = str(UUID())
#            record['_id'] = key
#            txn.put(key.encode(), dumps(record).encode(), db=self._db, append=True)
#            for _, index in self._index_array.items():
#                index.put(txn, key, record)
#        if txn: return put_actual()
#        with self._env.begin(write=True) as txn: return put_actual()

#    def put_raw(self, key, record):
#        with self._env.begin(write=True) as txn:
#            txn.put(key.encode(), dumps(record).encode(), db=self._db)

#    def get_raw(self, key):
#        with self._env.begin(write=True) as txn:
#            record = txn.get(key.encode(), db=self._db)
#            if not record: return None
#            return loads(bytes(record))

#    def upd(self, record, txn=None):
#        """put a new record into the table"""
#        def upd_actual():
#            key = record.get('_id', None)
#            if not key: return False
#            return txn.replace(key.encode(), dumps(record).encode(), db=self._db)
#        if txn: upd_actual()
#        with self._env.begin(write=True) as txn: upd_actual()

#    def delete(self, key, record, txn=None):
#        """remove a record from the table"""
#        def delete_actual():
#            txn.delete(key.encode(), db=self._db)
#            for _, index in self._index_array.items():
#                index.delete(txn, key, record)
#        if txn: delete_actual()
#        with self._env.begin(write=True) as txn: delete_actual()

#    @property
#    def count(self, txn=None):
#        """return a count of the number of records in the table"""
#        if txn: return txn.stat(self._db).get('entries', 0)
#        with self._env.begin(write=True) as txn: return txn.stat(self._db).get('entries', 0)

#    @property
#    def counts(self, txn=None):
#        """return a count of the number of records in each index"""
#        def counts_actual():
#            results = {'_id': txn.stat(self._db).get('entries', 0)}
#            for name, index in self._index_array.items():
#                results[name] = index.count
#            return results
#        if txn: return counts_actual()
#        with self._env.begin(write=True) as txn: return counts_actual()

#    def reindex(self, txn=None):
#        """regenerate all indexes"""
#        def reindex_actual():
#            for _, index in self._index_array.items():
#                index.drop(txn)
#                with Cursor(self._db, txn) as cursor:
#                    if not cursor.first(): continue
#                    while True:
#                        key, record = cursor.item()
#                        record = loads(bytes(record))
#                        index.put(txn, key, record)
#                        if not cursor.next(): break
#        if txn: return reindex_actual()
#        with self._env.begin(write=True) as txn: return reindex_actual()

#    def create_index(self, name, index):
#        """create a new index"""
#        if name in self._index_array:
#            return False
#        self._index_array[index['name']] = Index(self._env, index)
#        return index

#    def iterate_callback(self, data):
#        if data:
#            self.values.append(data)
#            for k,v in data.items():
#                kl = len(k)
#                vl = len(str(v))
#                mx = max(kl, vl)
#                if ((k in self.lengths) and (mx > self.lengths[k])) or (k not in self.lengths):
#                    self.lengths[k] = mx
#            return
#        #
#        separator = ''
#        fmt = ''
#        data = {}
#        #
#        for k,v in self.lengths.items():
#            separator += '+'+'-'*(v+2)
#        for k,v in self.lengths.items():
#            fmt += '| {'+k+':'+str(v)+'} '
#            data[k] = k
#        separator += '+'
#        fmt += '|'
#        print(separator)
#        print(fmt.format(**data))
#        print(separator)
#        for item in self.values:
#            print(fmt.format(**item))
#        print(separator)

#    def iterate(self, callback=None):
#        self.values = []
#        self.lengths = {}
#        count = 0
#        if not callback: callback = self.iterate_callback
#        with self._env.begin() as txn:
#            with Cursor(self._db, txn) as cursor:
#                if not cursor.first(): return
#                while True:
#                    key, record = cursor.item()
#                    record = loads(bytes(record))
#                    callback(record)
#                    if not cursor.next(): break
#                    count += 1
#                    if count>10: break
#        callback(None)

##############################################################################
#
#   CLASS :: Database
#
##############################################################################
#class Database(object):
#
#    _tables = {}
#    _config = {
#        'map_size': 1024*1024*1024 * 2,
#        'subdir': True,
#        'metasync': False,
#        'sync': True,
#        'max_dbs': 12,
#        'writemap': True
#    }
#    def __init__(self, *args, **argv):
#        _config = dict(self._config, **argv.get('env', {}))
#        self._env = Environment(args[0], **_config)
#        if argv.get('schema', 'yes') == 'yes':
#            self._schema = Schema(self)
#            self._schema.open()
#        self._schema = Table(self._env)

#    def __del__(self):
#        self.close()

#    def close(self):
#        if not self._env: return
#        self._env.close()
#        self._env = None

#    def open(self, name, indexes):
#        self._tables[name] = Table(self._env, name, indexes)
#        return self._tables[name]

#    def use(self, name):
#        return self._tables[name]

    #def schema(self):
    #    """return the schema for the current db"""
    #    return self._schema

    #def create_table(self, name):
    #    return self._schema.create_table(name)

    #def create_index(self, table, name, field, duplicates=False):
    #    """create a new index"""
    #    index = {
    #        'name': name,
    #        'func': field,
    #        'config': {'dupsort': duplicates}
    #    }
    #    return self._schema.create_index(table, name, index)

##############################################################################
#
#   CLASS :: Schema
#
##############################################################################
#class Schema(object):

#    _tables = {}
#    _schema = {}

#    def __init__(self, db):
#        self._db = db
#        self._table = db.open('_schema', {})
#        self._schema = self._table.get_raw('schema')
#        if not self._schema: self._schema = {}
#        if not 'tables' in self._schema: self._schema['tables'] = {}

#    def _flush(self):
#        self._table.put_raw('schema', self._schema)

#    def create_table(self, table_name):
#        if table_name in self._tables:
#            return {'status': 'fail', 'reason': 'table exists'}
#        self._schema['tables'][table_name] = {'indexes': {}}
#        self._flush()
#        self._db.open(table_name, {})
#        self._tables[table_name] = self._db.open(table_name, {})
#        return {'status': 'ok'}

#    def create_index(self, table_name, index_name, index):
#        if table_name not in self._schema['tables']:
#            return {'status': 'fail', 'reason': 'no such table'}
#        if index_name in self._schema['tables'][table_name]['indexes']:
#            return {'status': 'fail', 'reason': 'index exists'}
#        self._schema['tables'][table_name]['indexes'][index_name] = index
#        self._flush()
#        table = self._db.use(table_name)
#        table.open_index(index)
#        return {'status': 'ok'}

#    def open(self):
#        for table in self._schema['tables']:
#            indexes = self._schema['tables'][table]['indexes']
#            self._tables[table] = self._db.open(table, indexes)

#    def table(self, table):
#        if table not in self._tables:
#            self.create_table(table)
#        return self._tables[table]

#    def tables(self):
#        output = []
#        for name in self._tables:
#            output.append(name)
#        output.sort()
#        return output

#if __name__ == "main":
#    pass
#    # run test cases here ...

#db = Database('Sessions')
#mb = Schema(db)
#db.create_index('my_test', name='by_origin', field="(record):return record.get('origin','')", duplicates=True)
#schema.create_table('sessions')
#schema.create_table('origins')
#schema.open()
#print(schema._tables)
#origins = schema._tables['origins']
#print(origins)
#origins.iterate()
#db = Database('demodb')
#db.create_table('my_test')
#db.create_index('my_test', name='by_origin', field="(record):return record.get('origin','')", duplicates=True)
    #def iterate(self, max=None, callback=print):
    #    """
    #    iterate - public method to iterate through a recordset
    #    """
    #    with self._env.begin() as txn:
    #        with Cursor(self._db, txn) as cursor:
    #            if not cursor.first(): return
    #            count = 0
    #            while True:
    #                key, record = cursor.item()
    #                record = loads(bytes(record))
    #                callback(record)
    #                count += 1
    #                if not cursor.next() or (max and count>=max): break
