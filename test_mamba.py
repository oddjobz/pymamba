#!/usr/bin/python3

import unittest
from mamba import Database
from subprocess import call

class UnitTests(unittest.TestCase):

    _db_name = 'unit-db'
    _tb_name = 'demo1'
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

        self.assertEqual(table.records, len(data))
        self.assertEqual(table.index('by_name').count(), len(data))
        self.assertEqual(table.index('by_age').count(), len(data))

        table.delete([results[0]['_id']])
        table.delete([results[1]['_id']])
        table.delete([results[2]['_id']])

        self.assertEqual(table.records, len(data) - 3)
        self.assertEqual(table.index('by_name').count(), len(data) - 3)
        self.assertEqual(table.index('by_age').count(), len(data) - 3)

        results = table.find('by_age')
        last = 0
        for item in results:
            #print(item)
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
        results = table.find('by_age_name')
        for row in results:
            self.assertEqual(row['age'], ages.pop())

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

