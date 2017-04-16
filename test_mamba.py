#!/usr/bin/python3

import unittest
import lmdb
from pymamba import Database, Table, Index, _debug, xIndexMissing, \
    xWriteFail, xTableMissing
from subprocess import call


class UnitTests(unittest.TestCase):

    _db_name = 'unit-db'
    _tb_name = 'demo1'
    _debug = True
    _data = [
        {'name': 'Gareth Bult', 'age': 21, 'admin': True, 'cat': 'A'},
        {'name': 'Squizzey', 'age': 3000, 'cat': 'A'},
        {'name': 'Fred Bloggs', 'age': 45, 'cat': 'A'},
        {'name': 'John Doe', 'age': 40, 'admin': True, 'cat': 'B'},
        {'name': 'John Smith', 'age': 40, 'cat': 'B'},
        {'name': 'Jim Smith', 'age': 40, 'cat': 'B'},
        {'name': 'Gareth Bult1', 'age': 21, 'admin': True, 'cat': 'B'}
    ]

    def setUp(self):
        call(['rm', '-rf', self._db_name])

    def tearDown(self):
        pass

    def generate_data(self, db, table_name):
        table = db.table(table_name)
        for row in self._data:
            table.append(dict(row))

    def generate_data2(self, db, table_name):
        with db.begin():
            table = db.table(table_name)
            for row in self._data:
                table.append(dict(row))

    def test_01_open_database(self):
        db = Database(self._db_name)
        self.assertTrue(isinstance(db, Database))

    def test_02_create_table(self):
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.assertTrue(isinstance(table, Table))

    def test_03_tables(self):
        db = Database(self._db_name)
        db.table(self._tb_name)
        self.assertEqual(db.tables, ['demo1'])

    def test_04_exists(self):
        db = Database(self._db_name)
        db.table(self._tb_name)
        self.assertTrue(db.exists('demo1'))

    def test_05_drop(self):
        db = Database(self._db_name)
        db.table(self._tb_name)
        db.drop(self._tb_name)
        self.assertFalse(db.exists('demo1'))
        self.assertEqual(db.tables, [])

    def test_06_append(self):
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        for doc in self._data:
            table.append(dict(doc))
        self.assertEqual(table.records, len(self._data))

    def test_07_empty(self):
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        for doc in self._data:
            table.append(dict(doc))
        self.assertEqual(table.records, len(self._data))
        table.empty()
        self.assertEqual(table.records, 0)
        self.assertTrue(db.exists('demo1'))

    def test_08_delete(self):
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        for doc in self._data:
            table.append(dict(doc))
        doc = next(table.find(limit=1))
        table.delete(doc)
        self.assertEqual(table.records, len(self._data)-1)

    def test_09_create_drop_index(self):
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_name', '{name}')
        table.index('by_age', '{age:03}', duplicates=True)
        self.assertEqual(table.indexes, ['by_age', 'by_name'])
        db.drop(self._tb_name)
        self.assertEqual(db.tables, [])

    def test_10_put_data(self):

        people = {}
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_name', '{name}')
        table.index('by_age', '{age:03}', duplicates=True)
        for item in self._data:
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

        self.assertEqual(table.records, len(self._data))
        self.assertEqual(table.index('by_name').count(), len(self._data))
        self.assertEqual(table.index('by_age').count(), len(self._data))

        for record in table.find('by_age', limit=3):
            table.delete(record['_id'])

        self.assertEqual(table.records, len(self._data) - 3)
        self.assertEqual(table.index('by_name').count(), len(self._data) - 3)
        self.assertEqual(table.index('by_age').count(), len(self._data) - 3)

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

        db.drop(self._tb_name)
        self.assertEqual(db.tables, [])

    def test_11_compound_index(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', '{age:03}{name}')
        self.generate_data(db, self._tb_name)
        ages = [doc['age'] for doc in self._data]
        ages.sort()
        ages.reverse()

        for row in table.find('by_age_name'):
            self.assertEqual(row['age'], ages.pop())

        with self.assertRaises(ValueError):
            table.index('broken', '{')

    def test_12_table_reopen(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', '{age:03}{name}')
        table.index('by_name', '{name}')
        table.index('by_age', '{age:03}', duplicates=True)
        self.generate_data(db, self._tb_name)
        db.close()
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.assertEqual(['by_age', 'by_age_name', 'by_name'], table.indexes)

    def test_13_index_exists(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', '{age:03}{name}')
        table.index('by_name', '{name}')
        table.index('by_age', '{age:03}', duplicates=True)
        self.assertTrue(table.exists('by_name'))
        db.close()
        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.assertTrue(table.exists('by_name'))

    def test_14_table_empty(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', '{age:03}{name}')
        table.index('by_name', '{name}')
        table.index('by_age', '{age:03}', duplicates=True)
        self.generate_data(db, self._tb_name)
        self.assertEqual(table.records, len(self._data))
        table.empty()
        table = db.table(self._tb_name)
        self.assertEqual(table.records, 0)

    def test_15_check_append_exception(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', '{age:03}{name}')
        table.index('by_name', '{name}')
        table.index('by_age', '{age:03}', duplicates=True)
        self.generate_data(db, self._tb_name)
        table._indexes = 10
        before = table.records
        with self.assertRaises(xWriteFail):
            table.append({'_id': -1})
        after = table.records
        self.assertEqual(before, after)


    def test_16_check_delete_exception(self):

        class f(object):
            pass

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        with self.assertRaises(TypeError):
            table.delete([f])

    def test_17_check_drop_exception(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        with self.assertRaises(xTableMissing):
            db.drop('no table')

    def test_18_check_unindex_exception(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        with self.assertRaises(xIndexMissing):
            table.drop_index('fred')

        table.index('by_name', '{name}')
        self.assertTrue('by_name' in table.indexes)
        table.drop_index('by_name')
        self.assertFalse('by_name' in table.indexes)

        table.index('duff', '{name}')
        table._indexes['duff'] = None
        with self.assertRaises(AttributeError):
            table.drop_index('duff')

    def test_18_count_with_txn(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', '{age:03}{name}')
        table.index('by_name', '{name}')
        table.index('by_age', '{age:03}', duplicates=True)
        self.generate_data(db, self._tb_name)
        with db.env.begin() as txn:
            index = table.index('by_name')
            self.assertTrue(index.count(txn=txn), 7)

    def test_31_index_get(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', '{age:03}{name}')
        table.index('by_name', '{name}')
        table.index('by_age', '{age:03}', duplicates=True)
        self.generate_data(db, self._tb_name)
        with db._env.begin() as txn:
            index = table.index('by_name')
            _id = index.get(txn, {'name': 'Squizzey'})
            doc = table.get(_id)
        self.assertTrue(doc['age'], 3000)
        self.assertTrue(doc['name'], 'Squizzey')
        with self.assertRaises(xIndexMissing):
            list(table.find('fred', 'fred'))

    def test_32_filters(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        table.index('by_age_name', '{age:03}{name}')
        table.index('by_name', '{name}')
        self.generate_data(db, self._tb_name)
        result = list(table.find(expression=lambda doc: doc['age'] == 3000))[0]
        self.assertEqual(result['age'], 3000)
        self.assertEqual(result['name'], 'Squizzey')
        result = list(table.find('by_name', expression=lambda doc: doc['age'] == 21))[0]
        self.assertEqual(result['age'], 21)
        self.assertEqual(result['name'], 'Gareth Bult')
        result = list(table.find('by_name', expression=lambda doc: doc['name'] == 'John Doe'))[0]
        self.assertEqual(result['age'], 40)
        self.assertEqual(result['name'], 'John Doe')


    def test_33_reindex(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.generate_data(db, self._tb_name)
        by_age_name = table.index('by_age_name', '{age:03}{name}')
        by_name = table.index('by_name', '{name}')
        by_age = table.index('by_age', '{age:03}', duplicates=True)

        self.assertEqual(by_age_name.count(), 7)
        self.assertEqual(by_name.count(), 7)
        self.assertEqual(by_age.count(), 7)
        table.reindex()
        self.assertEqual(by_age_name.count(), 7)
        self.assertEqual(by_name.count(), 7)
        self.assertEqual(by_age.count(), 7)

    def test_34_function_index(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.generate_data(db, self._tb_name)
        table.index('by_compound', '{cat}|{name}', duplicates=True)
        table.index('by_age', '{age:03}', duplicates=True)

        results = []
        for doc in table.find('by_compound'):
            results.append(doc['cat'])
        self.assertEqual(results, ['A', 'A', 'A', 'B', 'B', 'B', 'B'])

        table.empty()
        table = db.table(self._tb_name)
        self.generate_data2(db, self._tb_name)

        results = []
        for doc in table.find('by_compound'):
            results.append(doc['cat'])
        self.assertEqual(results, ['A', 'A', 'A', 'B', 'B', 'B', 'B'])

        for i in table.seek('by_compound', {'cat': 'A', 'name': 'Squizzey'}):
            print("}}}",i)
            self.assertEqual(i['age'], 3000)

        for i in table.seek('by_compound', {'cat': 'B', 'name': 'John Doe'}):
            self.assertEqual(i['age'], 40)

        self.assertEqual(list(table.seek('by_compound', {'cat': 'C', 'name': 'Squizzey'})), [])

        lower = {'cat': 'A', 'name': 'Squizzey'}
        upper = {'cat': 'B', 'name': 'Gareth Bult1'}
        iter = table.range('by_compound', lower, upper)
        results = list(iter)

        self.assertEqual(results[0]['name'], 'Squizzey')
        self.assertEqual(results[1]['name'], 'Gareth Bult1')

        print(results[0])
        results[0]['name'] = '!Squizzey'
        results[0]['age'] = 1
        table.save(results[0])

        table._indexes['duff'] = None
        with self.assertRaises(AttributeError):
            table.save(results[0])

        self.assertEqual(list(table.find('by_compound'))[0]['name'], '!Squizzey')
        self.assertEqual(list(table.find('by_age'))[0]['age'], 1)

    def test_35_partial_index(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.generate_data(db, self._tb_name)
        table.index('by_admin', '{admin}', duplicates=True)
        try:
            for doc in table.find('by_admin'):
                print("> {admin}".format(**doc), doc)
        except Exception as error:
            self.fail('partial key failure')
            raise error

        self.assertEqual(table.index('by_admin').count(), 3)
        with self.assertRaises(AttributeError):
            table.unindex('by_admin', 123)

    def test_36_seek_one(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.generate_data(db, self._tb_name)
        table.index('by_age', '{age:03}', duplicates=True)
        doc = table.seek_one('by_age', {'age': 3000})
        self.assertEqual(doc['age'], 3000)
        self.assertEqual(doc['name'], 'Squizzey')
        print("!!!", doc)

    def test_37_drop_reuse(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        self.generate_data(db, self._tb_name)
        db.drop(self._tb_name)
        table = db.table(self._tb_name)
        self.generate_data(db, self._tb_name)
        table.index('by_age', '{age:03}', duplicates=True)
        doc = table.seek_one('by_age', {'age': 3000})
        self.assertEqual(doc['age'], 3000)
        self.assertEqual(doc['name'], 'Squizzey')
        for doc in table.find():
            _id = doc['_id']
            name = doc['name']
            break
        with db.begin():
            db.restructure(self._tb_name)
        table = db.table(self._tb_name)
        for doc in table.find():
            print(doc)
            self.assertEqual(doc['name'], name)
            self.assertEqual(doc['_id'], _id)
            break

    def test_38_range(self):

        db = Database(self._db_name)
        table = db.table(self._tb_name)
        data = [
            {'code': 'F', 'name': 'Tom'},
            {'code': 'E', 'name': 'Dick'},
            {'code': 'E', 'name': 'Dick1'},
            {'code': 'D', 'name': 'Harry'},
            {'code': 'C', 'name': 'Fred'},
            {'code': 'B', 'name': 'John'},
            {'code': 'B', 'name': 'John1'},
            {'code': 'A', 'name': 'Sam'},
        ]
        for row in data:
            table.append(row)

        table.index('by_code', '{code}', duplicates=True)
        res = list(table.find('by_code'))
        self.assertEqual(res[0]['code'], 'A')
        self.assertEqual(res[-1]['code'], 'F')

        res = list(table.find())
        lower = res[0]['_id']
        upper = res[-1]['_id']
        print('Lower={} Upper={}'.format(lower, upper))
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}))
        self.assertEqual(natural[0]['code'], 'F')
        self.assertEqual(natural[-1]['code'], 'A')

        res = list(table.find())
        lower = res[0]['_id']
        upper = res[-2]['_id']
        print('Lower={} Upper={}'.format(lower, upper))
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}))
        for doc in natural:
            print(doc)

        self.assertEqual(natural[0]['code'], 'F')
        self.assertEqual(natural[-1]['code'], 'B')

        res = list(table.find())
        lower = res[0]['_id']
        upper = res[-1]['_id']
        print('Lower={} Upper={}'.format(lower, upper))
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}, inclusive=False))
        for doc in natural:
            print(doc)

        self.assertEqual(natural[0]['code'], 'E')
        self.assertEqual(natural[-1]['code'], 'B')


        res = list(table.find())
        lower = None
        upper = res[-1]['_id']
        print('Lower={} Upper={}'.format(lower, upper))
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}))
        self.assertEqual(natural[0]['code'], 'F')
        self.assertEqual(natural[-1]['code'], 'A')

        res = list(table.find())
        lower = res[0]['_id']
        upper = None
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}))
        self.assertEqual(natural[0]['code'], 'F')
        self.assertEqual(natural[-1]['code'], 'A')

        res = list(table.find())
        lower = None
        upper = None
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}))
        self.assertEqual(natural[0]['code'], 'F')
        self.assertEqual(natural[-1]['code'], 'A')


        lower = res[0]['_id']
        upper = res[0]['_id']
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}))
        self.assertEqual(natural[0]['code'], 'F')

        lower = res[-1]['_id']
        upper = res[-1]['_id']
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}))
        self.assertEqual(natural[0]['code'], 'A')

        lower = res[0]['_id']
        upper = res[0]['_id']
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}, inclusive=False))
        self.assertEqual(natural, [])

        lower = res[-1]['_id']
        upper = res[-1]['_id']
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}, inclusive=False))
        self.assertEqual(natural, [])

        lower = res[0]['_id']
        upper = res[1]['_id']
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}, inclusive=False))
        self.assertEqual(natural, [])

        lower = res[0]['_id']
        upper = res[2]['_id']
        natural = list(table.range(None, {'_id': lower}, {'_id': upper}, inclusive=False))
        self.assertEqual(natural[0]['_id'], res[1]['_id'])

        table.index('by_code', '{code}')
        res = list(table.range('by_code', {'code': '0'}, {'code': 'Z'}))
        self.assertEqual(res[0]['code'], 'A')
        self.assertEqual(res[-1]['code'], 'F')

        res = list(table.range('by_code', {'code': 'B'}, {'code': 'E'}))
        self.assertEqual(res[0]['code'], 'B')
        self.assertEqual(res[-1]['code'], 'E')

        res = list(table.range('by_code', {'code': 'B'}, {'code': 'E'}, inclusive=False))
        self.assertEqual(res[0]['code'], 'C')
        self.assertEqual(res[-1]['code'], 'D')

        res = list(table.range('by_code', {'code': 'A'}, {'code': 'F'}, inclusive=False))
        self.assertEqual(res[0]['code'], 'B')
        self.assertEqual(res[-1]['code'], 'E')

        res = list(table.range('by_code', None, None))
        self.assertEqual(res[0]['code'], 'A')
        self.assertEqual(res[-1]['code'], 'F')


        #for doc in table.range('by_code', {'code': 'B'}, {'code': 'E'}, inclusive=False):
        #    print(doc)


