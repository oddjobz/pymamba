""" This is the current implementation of "mamba" which is a database layout
that sites directly on top of LMDB. Currently it's *much* faster than
Mongo, but currently incomplete and untested .. but it's great for 
playing with.
"""
##############################################################################
# TODO: We need an index-aware record update routine
# TODO: We need a search routine that can handle an index an a filter
# TODO: convert to use generators for search routines
# Test Coverage
# TODO: Index. !function
# TODO: Index.count - with txn
# TODO: Index.drop
# TODO: Index.get
# TODO: Exception on Table.append
# TODO: Exception on Table.delete
# TODO: Exception on Table.drop
# TODO: Table.find Exception with missing index
# TODO: Table.index Exception writing meta
# TODO: Table.unindex Exception
#
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
from traceback import print_stack

class Database(object):
    """
    Representation of a Database, this is the main API class
    
    :param name: The name of the database to open
    :type name: str
    :param conf: Any additional or custom options for this environment
    :type conf: dict  
    """
    _config = {
        'map_size': 1024*1024*1024 * 2,
        'subdir': True,
        'metasync': False,
        'sync': True,
        'max_dbs': 256,
        'writemap': True
    }
    def __init__(self, name, config={}):
        _config = dict(self._config, **config.get('env', {}))
        self._tables = {}
        self._env = Environment(name, **_config)
        self._db = self._env.open_db()

    def __del__(self):
        self.close()

    def close(self):
        """
        Close the current database
        """
        if not self._env: return
        self._env.close()
        self._env = None

    def exists(self, name):
        """
        Test whether a table with a given name already exists

        :param name: Table name
        :type name: str
        :return: True if table exists
        :rtype: bool
        """
        return name in self.tables

    def table(self, name):
        """
        Return a reference to a table with a given name, creating first if it doesn't exist
        
        :param name: Name of table
        :type name: str
        :return: Reference to table
        :rtype: Table
        """
        if name not in self._tables:
            self._tables[name] = Table(self._env, name)
        return self._tables[name]

    @property
    def tables(self):
        """
        PROPERTY - Generate a list of names of the tables associated with this database
        
        :getter: Returns a list of table names
        :type: list
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


class Index(object):
    """
    Representation of a table index created one per index when the table and it's indexes are opened.
    
    :param env: An LMDB Environment object
    :type env: Environment
    :param name: The name of the index we're working with
    :type name: str
    :param func: Can be a function used to generate index keys, or a field name
    :type func: str
    :param conf: Configuration options for this index
    :type conf: dict
     
    .. note:: if **func** begins with a **!** it is taken to be a function, otherwise
        it func is treated as a field name. The field type is dictated by the settings
        supplied in **conf**.
    """
    _debug = False
    _str_t = 'str(k["{}"]).encode()'
    _int_t = 'k["{}"].to_bytes(8,"big",signed=False)'

    def __init__(self, env, name, func, conf):
        self._env = env
        self._name = name
        self._conf = conf
        self._conf['key'] = self._conf['key'].encode()
        self._integer = conf.get('integerkey', False)
        if func[0] == '!':
            self._func = _anonymous(func[1:])
        else:
            if not isinstance(func, list):
                func = [func]
            fmt = ''
            names = []
            for item in func:
                if ':' in item:
                    fld, typ = item.split(':')
                else:
                    fld, typ = (item, str)
                if fmt:
                    fmt += "+b'|'+"
                fmt += self._int_t if typ == 'int' else self._str_t
                names.append(fld)
            fmt = '(k): return '+fmt
            #print(fmt.format(*names))
            self._func = _anonymous(fmt.format(*names))
        self._db = self._env.open_db(**self._conf)

    def count(self, txn=None):
        """
        Count the number of items currently present in this index
        
        :param txn: Is an open Transaction 
        :type txn: Transaction
        :return: The number if items in the index
        :rtype: int
        """
        def entries():
            return txn.stat(self._db).get('entries', 0)
        if txn:
            return entries()
        with self._env.begin() as txn:
            return entries()

    def cursor(self, txn):
        """
        Return a cursor into the current index

        :param txn: Is an open Transaction 
        :type txn: Transaction
        :return: An active Cursor object
        :rtype: Cursor       
        """
        return Cursor(self._db, txn)

    def delete(self, txn, key, record):
        """
        Delete the selected record from the current index
        
        :param txn: Is an open (write) Transaction
        :type txn: Transaction
        :param key: A database key
        :type key: str
        :param record: A currently existing record
        :type record: dict
        :return: True if the record was deleted
        :rtype: boolean
        """
        return txn.delete(self._func(record), key, db=self._db)

    def drop(self, txn):
        """
        Drop the current index

        :param txn: Is an open Transaction 
        :type txn: Transaction
        :return: The record recovered from the index
        :rtype: str
        """
        return txn.drop(self._db, delete=True)

    def get(self, txn, record):
        """
        Read a single record from the index
        
        :param txn: Is an open Transaction
        :type txn: Transaction
        :param record: Is a record template from which we can extract an index field
        :type record: dict
        :return: The record recovered from the index
        :rtype: str
        """
        return txn.get(self._func(record), db=self._db)

    def put(self, txn, key, record):
        """
        Write a new entry into the index
        
        :param txn: Is an open Transaction
        :type txn: Transaction
        :param key: Is the key to of the record to write
        :type key: str|int
        :param record: Is the record to write
        :type record: dict
        :return: True if the record was written successfully
        :rtype: boolean
        """
        return txn.put(self._func(record), key.encode(), db=self._db)


class Table(object):
    """
    Representation of a database table

    :param env: An open database Environment
    :type env: Environment
    :param name: A table name
    :type name: str   
    """
    _debug = False
    _indexes = {}

    def __init__(self, env, name=None):
        self._env = env
        self._name = name
        self._indexes = {}
        self._db = self._env.open_db(name.encode())
        for index in self.indexes:
            key = ''.join(['@', _index_name(self, index)]).encode()
            with self._env.begin() as txn:
                doc = loads(bytes(txn.get(key)))
                self._indexes[index] = Index(self._env, index, doc['func'], doc['conf'])

    def append(self, record):
        """
        Append a new record to this table
        
        :param record: The record to append
        :type record: dict
        :raises: lmdb_Aborted if transaction fails
        """
        try:
            key = str(UUID())
            record['_id'] = key
            with self._env.begin(write=True) as txn:
                txn.put(key.encode(), dumps(record).encode(), db=self._db, append=True)
                for name in self._indexes:
                    self._indexes[name].put(txn, key, record)

        except Exception as e:
            txn.abort()
            raise lmdb_Aborted(e)

    def delete(self, keys):
        """
        Delete a record from this table
        
        :param keys: A list of database keys to delete
        :type keys: list
        :raises: lmdb_Aborted on failure
        """
        try:
            with self._env.begin(write=True) as txn:
                for _id in keys:
                    key = _id.encode()
                    doc = loads(bytes(txn.get(key, db=self._db)))
                    txn.delete(key, db=self._db)
                    for name in self._indexes:
                        self._indexes[name].delete(txn, key, doc)
        except Exception as e:
            txn.abort()
            raise lmdb_Aborted(e)

    def drop(self, delete=True):
        """
        Drop this tablex and all it's indecies

        :param delete: Whether we delete the table after removing all items
        :type delete: bool
        :raises: lmdb_Aborted on failure
        """
        for name in self.indexes:
            self.unindex(name)

        try:
            with self._env.begin(write=True) as txn:
                txn.drop(self._db, delete)
        except Exception as e:
            txn.abort()
            raise lmdb_Aborted(e)

    def empty(self):
        """
        Clear all records from the current table

        :return: True if the table was cleared
        :rtype: bool
        """
        return self.drop(False)

    def exists(self, name):
        """
        See whether an index already exists or not

        :param name: Name of the index
        :type name: str
        :return: True if index already exists
        :rtype: bool
        """
        return name in self._indexes

    def get(self, key):
        """
        Get a single record based on it's key
        
        :param key: The _id of the record to get
        :type key: str
        :return: The requested record
        :rtype: dict
        :raises: lmdb_NotFound if record does not exist
        """
        try:
            with self._env.begin() as txn:
                return loads(bytes(txn.get(key, db=self._db)))
        except Exception as e:
            raise lmdb_NotFound(e)

    def find(self, name=None, max=None):
        """
        Find all records either sequentiall or based on an index
        
        :param name: The name of the index to use [OR use natural order] 
        :type name: str
        :param max: The maximum number of records to return
        :type max: int
        :return: The records that were located
        :rtype: list
        """
        results = []
        with self._env.begin() as txn:
            if not name:
                with Cursor(self._db, txn) as cursor:
                    if not cursor.first(): return []
                    count = 0
                    while True:
                        doc = cursor.value()
                        results.append(loads(bytes(doc)))
                        count += 1
                        if not cursor.next() or (max and count>=max): break
            else:
                if name not in self._indexes:
                    raise lmdb_IndexMissing(name)
                index = self._indexes[name]
                with index.cursor(txn) as cursor:
                    if not cursor.first(): return []
                    count = 0
                    while True:
                        doc = cursor.value()
                        doc = loads(bytes(txn.get(doc, db=self._db)))
                        results.append(doc)
                        count += 1
                        if not cursor.next() or (max and count>=max): break

        return results

    def index(self, name, func=None, duplicates=False, integer=False):
        """
        Return a reference for a names index, or create if not available

        :param name: The name of the index to create
        :type name: str
        :param func: A specification of the index, !<function>|<field name>
        :type func: str
        :param duplicates: Whether this index will allow duplicate keys
        :type duplicates: bool
        :param integer: Whether this index has integer keys (or string keys)
        :type integer: bool
        :return: A reference to the index, created index, or None if index creation fails
        :rtype: Index
        :raises: lmdb_Aborted on error
        """
        if name not in self._indexes:
            conf = {
                'key': _index_name(self, name),
                'integerkey': integer,
                'integerdup': duplicates,
                'dupsort': duplicates,
                'create': True,
            }
            try:
                self._indexes[name] = Index(self._env, name, func, conf)
            except Exception as e:
                raise lmdb_Aborted(e)

            try:
                with self._env.begin(write=True) as txn:
                    if func == '__killme__': key = nothing
                    key = ''.join(['@', _index_name(self, name)]).encode()
                    val = dumps({'conf': conf, 'func': func}).encode()
                    txn.put(key, val)
                    # TODO: Implement reindex function
                    # self._indexes[name].reindex()
            except Exception as e:
                txn.abort()
                raise e

        return self._indexes[name]

    def unindex(self, name):
        """
        Delete the named index

        :param name: The name of the index
        :type name: str
        :raises: lmdb_Aborted on error
        :raises: lmdb_IndexMissing if the index does not exist
        """
        if name not in self._indexes:
            raise lmdb_IndexMissing()

        try:
            with self._env.begin(write=True) as txn:
                if name == '__killme__': key = nothing
                self._indexes[name].drop(txn)
                txn.delete(''.join(['@', _index_name(self, name)]).encode())
        except Exception as e:
            txn.abort()
            raise e

    @property
    def indexes(self):
        """
        PROPERTY - Recover a list of indexes for this table

        :getter: The indexes for this table
        :type: list
        """
        results = []
        index_name = _index_name(self, '')
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
    def records(self):
        """
        PROPERTY - Recover the number of records in this table

        :getter: Record count
        :type: int
        """
        with self._env.begin() as txn:
            return txn.stat(self._db).get('entries', 0)

def _debug(self, msg):
    """
    Display a debug message with current line number and function name

    :param self: A reference to the object calling this routine
    :type self: object
    :param msg: The message you wish to display
    :type msg: str
    """
    if not self._debug: return
    line = _getframe(1).f_lineno
    name = _getframe(1).f_code.co_name
    print("{}: #{} - {}".format(name, line, msg))


def _anonymous(text):
    """
    An anonymous function used to generate functions for database indecies

    :param text: The body of the function call to generate
    :type text: str
    """
    scope = {}
    exec('def func{0}'.format(text), scope)
    return scope['func']

def _index_name(self, name):
    """
    Generate the name of the object in which to store index records

    :param name: The name of the table
    :type name: str
    :return: A string representation of the full table name 
    :rtype: str
    """
    return '_{}_{}'.format(self._name, name)


class lmdb_TableExists(Exception):
    """Exception - database table already exists"""
    pass


class lmdb_IndexExists(Exception):
    """Exception - index already exists"""
    pass


class lmdb_TableMissing(Exception):
    """Exception - database table does not exist"""
    pass


class lmdb_IndexMissing(Exception):
    """Exception - index does not exist"""
    pass


class lmdb_NotFound(Exception):
    """Exception - expected record was not found"""
    pass

class lmdb_Aborted(Exception):
    """Exception - transaction did not complete"""

