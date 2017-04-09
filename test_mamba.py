#!/usr/bin/python3

import unittest
from mamba import Database, _debug, \
    lmdb_Aborted, lmdb_IndexExists, lmdb_IndexMissing, lmdb_IndexExists,\
    lmdb_NotFound, lmdb_TableExists, lmdb_TableMissing
from subprocess import call

class UnitTests(unittest.TestCase):

    _db_name = 'unit-db'
    _tb_name = 'demo1'
    _debug = True
    _data = [
        {'name': 'Gareth Bult', 'age': 21},
        {'name': 'Squizzey', 'age': 3000},
        {'name': 'Fred Bloggs', 'age': 45},
        {'name': 'John Doe', 'age': 40},
        {'name': 'John Smith', 'age': 40},
        {'name': 'Jim Smith', 'age': 40},
        {'name': 'Gareth Bult1', 'age': 21}
    ]

    def setUp(self):
        call(['rm', '-rf', self._db_name])

    def tearDown(self):
        pass

    def generate_data(self, db, table_name):
        table = db.table(table_name)
        for row in self._data:
            table.append(row)

    def test_01_basic(self):
        db = Database(self._db_name)
        self.assertEqual(db.tables, [])

    def test_02_create_drop(self):
        db = Database(self._db_name)
        self.assertEqual(db.tables, [])
        table = db.table(self._tb_name)
        self.assertEqual(db.tables, [self._tb_name])
        table.drop(True)
        self.assertEqual(db.tables, [])

    def test_03_create_drop_index(self):
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_name', 'name')
        table.index('by_age', 'age:int', integer=True, duplicates=True)
        self.assertEqual(table.indexes, ['by_age', 'by_name'])
        table.drop(True)
        self.assertEqual(db.tables, [])

    def test_04_put_data(self):

        data = [
            {'name': 'Gareth Bult', 'age': 21},
            {'name': 'Squizzey', 'age': 3000},
            {'name': 'Fred Bloggs', 'age': 45},
            {'name': 'John Doe', 'age': 40},
            {'name': 'John Smith', 'age': 40},
            {'name': 'Jim Smith', 'age': 40},
            {'name': 'Gareth Bult1', 'age': 21}
        ]
        people = {}

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_name', 'name')
        table.index('by_age', 'age:int', integer=True, duplicates=True)
        for item in data:
            table.append(item)
            people[item['name']] = item
        for item in table.find():
            key = item.get('name', None)
            self.assertIsNotNone(key)
            if key:
                person = people.get(key, None)
                self.assertIsNotNone(person)
                if person:
                    self.assertEqual(person['age'], item['age'])
                    self.assertEqual(person['_id'], item['_id'])

        last = ''
        for item in table.find('by_name'):
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

        last = 0
        for item in table.find('by_age'):
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

        self.assertEqual(table.records, len(data))
        self.assertEqual(table.index('by_name').count(), len(data))
        self.assertEqual(table.index('by_age').count(), len(data))

        for record in table.find('by_age', limit=3):
            table.delete(record['_id'])

        self.assertEqual(table.records, len(data) - 3)
        self.assertEqual(table.index('by_name').count(), len(data) - 3)
        self.assertEqual(table.index('by_age').count(), len(data) - 3)

        last = 0
        for item in table.find('by_age'):
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

        table.drop(True)
        self.assertEqual(db.tables, [])

    def test_20_compound_index(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', ['age:int', 'name'])
        self.generate_data(db, self._tb_name)
        ages = [doc['age'] for doc in self._data]
        ages.sort()
        ages.reverse()

        for row in table.find('by_age_name'):
            self.assertEqual(row['age'], ages.pop())

        with self.assertRaises(SyntaxError):
            table.index('broken', '!')
        table.drop(True)

    def test_21_table_reopen(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', ['age:int', 'name'])
        table.index('by_name', 'name')
        table.index('by_age', 'age:int', integer=True, duplicates=True)
        self.generate_data(db, self._tb_name)
        db.close()
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.assertEqual(['by_age', 'by_age_name', 'by_name'], table.indexes)

    def test_22_table_exists(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.assertTrue(db.exists(self._tb_name))

    def test_23_try_debug(self):

        _debug(self, 'We are here!')

    def test_24_index_exists(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', ['age:int', 'name'])
        table.index('by_name', 'name')
        table.index('by_age', 'age:int', integer=True, duplicates=True)
        self.assertTrue(table.exists('by_name'))
        db.close()
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.assertTrue(table.exists('by_name'))

    def test_25_table_empty(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', ['age:int', 'name'])
        table.index('by_name', 'name')
        table.index('by_age', 'age:int', integer=True, duplicates=True)
        self.generate_data(db, self._tb_name)
        self.assertEqual(table.records, len(self._data))
        table.empty()
        table = db.table(self._tb_name)
        self.assertEqual(table.records, 0)

    def test_26_check_append_exception(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', ['age:int', 'name'])
        table.index('by_name', 'name')
        table.index('by_age', 'age:int', integer=True, duplicates=True)
        self.generate_data(db, self._tb_name)
        table._indexes = 10
        with self.assertRaises(TypeError):
            table.append({'_id': -1})

    def test_27_check_index_exception(self):

        class f(object):
            pass

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        with self.assertRaises(TypeError):
            table.index('by_name', f)


    def test_28_check_delete_exception(self):

        class f(object):
            pass

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        with self.assertRaises(AttributeError):
            table.delete([f])

    def test_29_check_drop_exception(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        with self.assertRaises(TypeError):
            table._db = None
            table.drop()

    def test_29_check_unindex_exception(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        with self.assertRaises(lmdb_IndexMissing):
            table.unindex('fred')

        table.index('by_name', 'name')
        self.assertTrue('by_name' in table.indexes)
        table.unindex('by_name')
        self.assertFalse('by_name' in table.indexes)

    def test_30_count_with_txn(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', ['age:int', 'name'])
        table.index('by_name', 'name')
        table.index('by_age', 'age:int', integer=True, duplicates=True)
        self.generate_data(db, self._tb_name)
        with db._env.begin() as txn:
            index = table.index('by_name')
            self.assertTrue(index.count(txn), 7)

    def test_31_index_get(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', ['age:int', 'name'])
        table.index('by_name', 'name')
        table.index('by_age', 'age:int', integer=True, duplicates=True)
        self.generate_data(db, self._tb_name)
        with db._env.begin() as txn:
            index = table.index('by_name')
            _id = index.get(txn, {'name': 'Squizzey'})
            doc = table.get(_id)
        self.assertTrue(doc['age'], 3000)
        self.assertTrue(doc['name'], 'Squizzey')
        with self.assertRaises(lmdb_IndexMissing):
            list(table.find('fred', 'fred'))

    def test_32_filters(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', ['age:int', 'name'])
        table.index('by_name', 'name')
        self.generate_data(db, self._tb_name)
        result = list(table.find(filter=lambda doc: doc['age'] == 3000))[0]
        self.assertEqual(result['age'], 3000)
        self.assertEqual(result['name'], 'Squizzey')
        result = list(table.find('by_name', filter=lambda doc: doc['age'] == 21))[0]
        self.assertEqual(result['age'], 21)
        self.assertEqual(result['name'], 'Gareth Bult')
        result = list(table.find('by_name', filter=lambda doc: doc['name'] == 'John Doe'))[0]
        self.assertEqual(result['age'], 40)
        self.assertEqual(result['name'], 'John Doe')


    def test_33_reindex(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.generate_data(db, self._tb_name)
        by_age_name = table.index('by_age_name', ['age:int', 'name'])
        by_name = table.index('by_name', 'name')
        by_age = table.index('by_age', 'age:int', integer=True, duplicates=True)
        self.assertEqual(by_age_name.count(), 7)
        self.assertEqual(by_name.count(), 7)
        self.assertEqual(by_age.count(), 7)
        by_age_name.reindex(table._db)
        by_name.reindex(table._db)
        by_age.reindex(table._db)
        self.assertEqual(by_age_name.count(), 7)
        self.assertEqual(by_name.count(), 7)
        self.assertEqual(by_age.count(), 7)

