#!/usr/bin/python3

import unittest
import pytest
from pymamba import Database
from pymamba.models import BaseModel, ManyToMany
from pymamba.types import DateType, AgeType, NameType, UUIDType
from subprocess import call
from time import sleep

class UserModel(BaseModel):
    """
    Model definition for objects of type 'UserModel'

    _calculated holds field definitions for all customised fields we want to use.
    _display holds field definitions for the 'list' function
    """
    _calculated = {
        'uuid': UUIDType('_id'),
        'dob_ddmmyyyy': DateType('dob'),
        'age': AgeType('dob'),
        'name': NameType(('forename', 'surname'))
    }
    _display = [
        {'name': 'name', 'width': 30, 'precision': 30},
        {'name': 'age', 'width': 3},
        {'name': 'dob_ddmmyyyy', 'width': 10},
        {'name': 'postcodes', 'width': 20, 'precision': 20, 'function': '_postcodes'}
    ]

    def _postcodes(self):
        self.__setattr__('postcodes', 'Ok')


class AddressModel(BaseModel):

    _calculated = {
        'uuid': UUIDType('_id')
    }
    _display = [
        {'name': 'uuid', 'width': 24},
        {'name': 'line1', 'width': 30, 'precision': 30},
        {'name': 'line2', 'width': 30, 'precision': 30},
        {'name': 'line3', 'width': 30, 'precision': 30},
        {'name': 'line4', 'width': 30, 'precision': 30},
        {'name': 'postcode', 'width': 9, 'precision': 9},
    ]


class UnitTests(unittest.TestCase):

    def setUp(self):
        call(['rm', '-rf', "databases/test_orm"])
        self._database = Database('databases/test_orm', {'env': {'map_size': 1024 * 1024 * 10}})
        self._user_model = UserModel(table=self._database.table('users'))
        self._address_model = AddressModel(table=self._database.table('addresses'))
        self._links = ManyToMany(self._database, self._user_model, self._address_model)

    def tearDown(self):
        self._database.close()

    def generate_data_1(self):
        self._user_model.add({"forename":"tom", "surname": "smith", "dob_ddmmyyyy": "01/01/1971", "uid": 1})
        self._user_model.add({"forename":"dick", "surname": "smith", "dob_ddmmyyyy": "01/01/1972", "uid": 2})
        self._user_model.add({"forename":"harry", "surname": "smith", "dob_ddmmyyyy": "01/01/1973", "uid": 3})
        self._user_model.add({"forename":"sally", "surname": "jones", "dob_ddmmyyyy": "01/01/1969", "uid": 4})
        self._user_model.add({"forename":"mary", "surname": "jones", "dob_ddmmyyyy": "01/01/1968", "uid": 5})
        self._user_model.add({"forename":"sophie", "surname": "jones", "dob_ddmmyyyy": "01/01/1967", "uid": 6})
        self._user_model.add({"forename":"joker", "surname": "wildcard", "dob_ddmmyyyy": "01/01/1966", "uid": 7})
        self._address_model.add({"line1":"Address line 1 # 1", "line2": "Address line 21","postcode":"CFXX 1AA"})
        self._address_model.add({"line1":"Address line 1 # 2", "line2": "Address line 22","postcode":"CFXX 1BB"})
        self._address_model.add({"line1":"Address line 1 # 3", "line2": "Address line 23","postcode":"CFXX 1CC"})
        self._address_model.add({"line1":"Address line 1 # 4", "line2": "Address line 24","postcode":"CFXX 1DD"})

    @pytest.fixture(autouse=True)
    def capfd(self, capfd):
        self.capfd = capfd

    def test_check_records(self):
        self.generate_data_1()
        self.assertEqual(self._user_model._table.records, 7)
        self.assertEqual(self._address_model._table.records, 4)

    def test_add_new_address(self):
        self.generate_data_1()
        doc = list(self._user_model.find())[0]
        doc.addresses.append(AddressModel({"line1": "Address line 1 # 5", "line2": "Address line 25","postcode": "CFXX 1DE"}))
        doc.save()
        doc = list(self._user_model.find())[0]
        doc.addresses.append(AddressModel({"line1": "Address line 1 # 6", "line2": "Address line 26", "postcode":"CFXX 1DF"}))
        doc.save()
        doc = list(self._user_model.find())[0]
        self.assertEqual(doc.addresses[0].postcode, 'CFXX 1DE')
        self.assertEqual(doc.addresses[1].postcode, 'CFXX 1DF')

        self._database.drop('users')
        self._database.drop('addresses')
        self._user_model = UserModel(table=self._database.table('users'))
        self._address_model = AddressModel(table=self._database.table('addresses'))

    def test_update_address(self):
        self.generate_data_1()
        doc = list(self._user_model.find())[0]
        doc.addresses.append({"line1":"Address line 1 # 5", "line2": "Address line 25","postcode":"CFXX 1DE"})
        doc.addresses.append({"line1":"Address line 1 # 6", "line2": "Address line 26", "postcode":"CFXX 1DF"})
        doc.save()
        doc = list(self._user_model.find())[0]
        doc.addresses[0].postcode += '!'
        doc.addresses[1].postcode += '!'
        doc.save()
        doc = list(self._user_model.find())[0]
        self.assertEqual(doc.addresses[0].postcode, 'CFXX 1DE!')
        self.assertEqual(doc.addresses[1].postcode, 'CFXX 1DF!')

    def test_04_delete_address(self):
        self.generate_data_1()
        doc = list(self._user_model.find())[0]
        doc.addresses.append(
            AddressModel({"line1": "Address line 1 # 5", "line2": "Address line 25", "postcode": "CFXX 1DE"},
                         table=self._address_model._table))
        doc.addresses.append(
            AddressModel({"line1": "Address line 1 # 6", "line2": "Address line 26", "postcode": "CFXX 1DF"},
                         table=self._address_model._table))
        doc.save()
        doc = list(self._user_model.find())[0]
        del doc.addresses[0]
        doc.save()
        doc = list(self._user_model.find())[0]
        self.assertEqual(doc.addresses[0].postcode, 'CFXX 1DF')
        self.assertEqual(len(doc.addresses), 1)

    def test_formatting_function(self):
        self.generate_data_1()

        self._user_model.list()
        out, err = self.capfd.readouterr()
        with open('tests/test_orm_table1.txt') as io:
            compare = io.read()
        self.assertEqual(compare, out)

    def test_add_with_linked(self):
        self.generate_data_1()
        doc = list(self._user_model.find())[0]
        doc.addresses.append({'line1': 'NEW LINE', 'postcode': 'NEW'})
        doc.save()
        doc = list(self._user_model.find())[0]
        self.assertEqual(doc.addresses[0].postcode, 'NEW')
        self.assertEqual(len(doc.addresses), 1)



    #def test_check_get(self):
    #    self.generate_data_1()
    #    for doc in self._model.find():
    #        get = self._model.get(doc._id)
    #        self.assertEqual(str(doc), str(get))
    #    self.assertEqual(len(doc.fred), 0)

    #def test_check_set(self):
    #    self.generate_data_1()
    #    for doc in self._model.find():
    #        doc.surname = 'CHANGED'
    #        doc.save()
    #        get = self._model.get(doc._id)
    #        self.assertEqual(get.surname, "CHANGED")

    #def test_check_modify(self):
    #    self.generate_data_1()
    #    for doc in self._model.find():
    #        self._model.modify(doc.uuid, 'forename=ME')
    #        get = self._model.get(doc._id)
    #        self.assertEqual(get.forename, "ME")

    #def test_check_list(self):
    #    self.generate_data_1()
    #    self._model.list()
    #    out, err = self.capfd.readouterr()
    #    with open('tests/test_models_table1.txt') as io:
    #        compare = io.read()
     #   self.assertEqual(compare, out)
     #   for doc in self._model.find():
     #       self._model.list(str(doc._id.decode()))
     #       break
     #   out, err = self.capfd.readouterr()
     #   with open('tests/test_models_table2.txt') as io:
     #       compare = io.read()
     #   self.assertEqual(compare, out)

    #def test_data_type_name(self):
    #    age = AgeType('dob')
    #    self.assertEqual(age.name, 'dob')

    #def test_from_internal(self):
    #    name = BaseType('name')
    #    doc = {'name': 'fred'}
    #    self.assertEqual(name.from_internal(doc), 'fred')

    #def test_to_internal(self):
    #    name = BaseType('name')
    #    doc = {'name': 'fred'}
    #    name.to_internal(doc, 'jim')
    #    self.assertEqual(name.from_internal(doc), 'jim')

if __name__ == "__main__":
    test = UnitTests()
    test.setUp()
    test.generate_data_1()
    test._model.list()
