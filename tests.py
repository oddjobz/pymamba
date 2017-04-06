#!/usr/bin/python3

import unittest
from mamba import Database
from subprocess import call

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
        db.create_index('demo1', 'by_name', 'name')
        db.create_index('demo1', 'by_age', 'age', integer=True, duplicates=True)
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

        self.assertEqual(table.count, len(data))
        self.assertEqual(table.count_index('by_name'), len(data))
        self.assertEqual(table.count_index('by_age'), len(data))

        table.delete([results[0]['_id']])
        table.delete([results[1]['_id']])
        table.delete([results[2]['_id']])

        self.assertEqual(table.count, len(data) - 3)
        self.assertEqual(table.count_index('by_name'), len(data) - 3)
        self.assertEqual(table.count_index('by_age'), len(data) - 3)

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

