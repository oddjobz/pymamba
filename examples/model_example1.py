from sys import argv
from pymamba import Database
from pymamba.models import BaseModel
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
        {'name': 'uuid', 'width': 24}
    ]


if __name__ == '__main__':

    database = Database('databases/people_database', {'env': {'map_size': 1024 * 1024 * 10}})
    user_model = UserModel(table=database.table('users'))
    #
    #   Really basic interface using functions built-in to the BaseModel class.
    #
    commands = {
        'lst': user_model.list,
        'add': user_model.append,
        'mod': user_model.modify,
    }
    try:
        commands[argv[1]](*argv[2:])
        exit()
    except IndexError:
        print('Insufficient parameters')
    except KeyError:
        print('No such command "{}"'.format(argv[1]))
    except Exception as e:
        raise e
    print('Usage: {} [{}]'.format(argv[0], '|'.join(list(commands.keys()))))
