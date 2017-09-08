from sys import argv
from pymamba import Database
from pymamba.models import BaseModel, ManyToMany
from pymamba.types import DateType, AgeType, NameType, UUIDType


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
        {'name': 'uuid', 'width': 24},
        {'name': 'postcodes', 'width': 60, 'precision': 60, 'function': '_postcodes'}
    ]


    def _postcodes(self):
        pcs = ''
        for address in self.addresses:
            if len(pcs):
                pcs += ' | '
            pcs += address.postcode
        self.__setattr__('postcodes', format('[{}] {}'.format(len(self.addresses), str(pcs))))


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


def modify():

    for doc in user_model.find():
        #doc.addresses.append(AddressModel({'line1': 'New Address', 'line2': '456', 'postcode': 'New Postcode'}, table=address_model._table))
        #print(doc.addresses)
        #doc.save()
        doc.addresses[0].postcode = 'CF38 1AD'
        doc.save()
        break
        for address in doc.addresses:
            #if address.postcode == 'CF38 1AA':
            print("id_={d.uuid} pc={d.postcode}".format(d=address))
            doc.addresses.remove(address)
            del doc.addresses[0]
                #address.line1 = 'Gareth woz ere'
            doc.save()
            break
        break


if __name__ == '__main__':

    database = Database('databases/people_database', {'env': {'map_size': 1024 * 1024 * 10}})
    user_model = UserModel(table=database.table('users'))
    address_model = AddressModel(table=database.table('addresses'))
    user_address = ManyToMany(database, user_model, address_model)
    #
    #   Really basic interface using functions built-in to the BaseModel class.
    #
    commands = {
        'lst': user_model.list,
        'add': user_model.append,
        'mod': user_model.modify,
        'add_address': address_model.append,
        'mod_address': address_model.modify,
        'lst_address': address_model.list
    }
    #try:
    commands[argv[1]](*argv[2:])
    modify()
    exit()
    #except IndexError:
    #    print('Insufficient parameters')
    #except KeyError:
    #    print('No such command "{}"'.format(argv[1]))
    #except Exception as e:
    #    raise e
    #print('Usage: {} [{}]'.format(argv[0], '|'.join(list(commands.keys()))))
