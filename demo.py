from pymamba import Database
from pymamba.models import ManyToMany, Table
from pymamba.types import AgeType, DateType
import datetime


class UserModel(Table):
    _calculated = {
        'age': AgeType('dob'),
        'birthday': DateType('dob')
    }
    _display = [
        {'name': 'forename', 'width': 20},
        {'name': 'surname', 'width': 20},
        {'name': 'birthday', 'width': 15},
        {'name': 'age', 'width': 3}
    ]


class AddressModel(Table):
    _display = [
        {'name': 'address', 'width': 30},
        {'name': 'postcode', 'width': 15}
    ]
    
db = Database('my_db', {'env': {'map_size': 1024 * 1024 * 10}})
user_model = UserModel(table=db.table('users'))
address_model = AddressModel(table=db.table('addresses'))
links = ManyToMany(db, user_model, address_model)

