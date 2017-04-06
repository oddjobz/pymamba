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
from traceback import print_stack


class Database(object):
    """Representation of a Database, this is the main API class
    
    :param database: The name of the database to open
    :type database: str
    :param configuration: Any additional or custom options for this environment
    :type configuration: dict
    """
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

    def clear(self, name):
        """Clear all records from the named table
        
        :param name: The table name
        :type name: str
        :return: True if the table was cleared
        :rtype: bool
        """
        return self.drop(name, False)

    def close(self):
        """Close the current database
        """
        if not self._env: return
        self._env.close()
        self._env = None

    def create_index(self, table_name, index_name, func, duplicates=False, integer=True):
        """Create a new index on the named table
        
        :param table_name: The name of the table to operate on
        :type table_name: str
        :param index_name: The name of the new index
        :type index_name: str
        :param func: Can be a function used to generate index keys, or a field name
        :type func: str
        :param duplicates: Whether we allow duplicate keys 
        :type duplicates: bool
        :param integer: Whether we are an integer key (or string)
        :type integer: bool
        :return: True if the index was created successfully
        :rtype: bool
        """
        if not self.table_exists(table_name):             raise lmdb_TableMissing(table_name)
        if self.index_exists(table_name, index_name):     raise lmdb_IndexExists(index_name)
        return self.table(table_name).create_index(index_name, func, duplicates, integer)

    def delete_index(self, table_name, index_name):
        """Delete the named index
        
        :param table_name: The name of the table to operate on
        :type table_name: str
        :param index_name: The name of the index to delete
        :type index_name: str
        :return: True if the index was deleted successfully
        :rtype: bool
        """
        if not self.table_exists(table_name):             raise lmdb_TableMissing(table_name)
        if not self.index_exists(table_name, index_name): raise lmdb_IndexMissing(index_name)
        return self.table(table_name).delete_index(index_name)

    def drop(self, name, delete=True):
        """Drop the named table and associated indecies

        :param name: Table to drop
        :type name: str
        :param delete: Whether we delete the table after removing all items
        :type delete: bool
        :return: True if the table was successfully dropped
        :rtype: bool
        """
        if not self.table_exists(name): raise lmdb_TableMissing(name)
        table = self.table(name)
        for index in table.indexes: table.delete_index(index)
        db = self._env.open_db(name.encode())
        try:
            with self._env.begin(write=True) as txn:
                txn.drop(db, delete)
        except Exception:
            print("~~ exception :: transaction aborted ~~")
            print_stack()
            txn.abort()
        return True

    def index_exists(self, table_name, index_name):
        key = '_{}_{}'.format(table_name, index_name)
        return key in self.tables

    def indexes(self, name):
        """
        indexes - public method to get the indexes for a table
        """
        return self.table(name).indexes

    def table(self, name):
        """
        table - public method to get a handle for a table
        """
        if name not in self._tables:
            self._tables[name] = Table(self._env, name)
        return self._tables[name]

    def table_exists(self, table_name):
        return table_name in self.tables

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


class Index(object):
    """Representation of a table index created one per index when the table and
    it's indexes are opened.
    
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

    def count(self, txn):
        """Count the number of items currently present in this index
        
        :param txn: Is an open Transaction 
        :type txn: Transaction
        :return: The number if items in the index
        :rtype: int
        """
        return txn.stat(self._db).get('entries', 0)

    def cursor(self, txn):
        """Return a cursor into the current index

        :param txn: Is an open Transaction 
        :type txn: Transaction
        :return: An active Cursor object
        :rtype: Cursor       
        """
        return Cursor(self._db, txn)

    def delete(self, txn, key, record):
        """Delete the selected record from the current index
        
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
        """Drop the current index

        :param txn: Is an open Transaction 
        :type txn: Transaction
        :return: The record recovered from the index
        :rtype: str
        """
        return txn.drop(self._db, delete=False)

    def get(self, txn, record):
        """Read a single record from the index
        
        :param txn: Is an open Transaction
        :type txn: Transaction
        :param record: Is a record template from which we can extract an index field
        :type record: dict
        :return: The record recovered from the index
        :rtype: str
        """
        return txn.get(self._func(record), db=self._db)

    def put(self, txn, key, record):
        """Write a new entry into the index
        
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
    """Representation of a database table

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
            key = ''.join(['@', self._index_name(index)]).encode()
            with self._env.begin() as txn:
                doc = loads(bytes(txn.get(key)))
                self._indexes[index] = Index(self._env, index, doc['func'], doc['conf'])

    def _index_name(self, name):
        """Generate the name of the object in which to store index records
        
        :param name: The name of the table
        :type name: str
        :return: A string representation of the full table name 
        :rtype: str
        """
        return '_{}_{}'.format(self._name, name)

    def count_index(self, name):
        """Count the number of entries in a named index
        
        :param name: The name of the index
        :type name: str
        :return: The current number of entires in the index
        :rtype: int
        """
        with self._env.begin() as txn:
            return self._indexes[name].count(txn)

    def create_index(self, name, func, duplicates=False, integer=False):
        """Create a new index for on this table
        
        :param name: The name of the index to create
        :type name: str
        :param func: A specification of the index, !<function>|<field name>
        :type func: str
        :param duplicates: Whether this index will allow duplicate keys
        :type duplicates: boolean
        :param integer: Whether this index has integer keys (or string keys)
        :type integer: boolean
        :return: True if the index was created
        :rtype: boolean
        """
        conf = {
            'key': self._index_name(name),
            'integerkey': integer,
            'integerdup': duplicates,
            'dupsort': duplicates,
            'create': True,
        }
        self._indexes[name] = Index(self._env, name, func, conf)
        try:
            with self._env.begin(write=True) as txn:
                key = ''.join(['@', self._index_name(name)]).encode()
                val = dumps({'conf': conf, 'func': func}).encode()
                txn.put(key, val)
                #self._indexes[name].reindex()
                return True
        except Exception:
            print("~~ exception :: transaction aborted ~~")
            print_stack()
            txn.abort()
            return False

    def delete_index(self, name):
        """Delete the names index
        
        :param name: The name of the index
        :type name: str
        :return: True if the index was deleted successfully
        :rtype: boolean
        """
        db = self._env.open_db(self._index_name(name).encode())
        try:
            with self._env.begin(write=True) as txn:
                txn.drop(db, True)
                txn.delete(''.join(['@', self._index_name(name)]).encode())
        except Exception:
            print("~~ exception :: transaction aborted ~~")
            print_stack()
            txn.abort()

    def append(self, doc):
        """Append a new record to this table
        
        :param doc: The record to append
        :type doc: dict
        :return: True if the record was successfully appended
        :rtype: bool
        """
        key = str(UUID())
        doc['_id'] = key
        try:
            with self._env.begin(write=True) as txn:
                txn.put(key.encode(), dumps(doc).encode(), db=self._db, append=True)
                for name in self._indexes:
                    self._indexes[name].put(txn, key, doc)
        except Exception:
            print("~~ exception :: transaction aborted ~~")
            print_stack()
            txn.abort()

    @property
    def count(self):
        """Recover the number of entries in this table
        
        :return: The number of entries
        :rtype: int
        """
        with self._env.begin() as txn:
            return txn.stat(self._db).get('entries', 0)

    def delete(self, ids):
        """Delete a record from this table
        
        :param ids: A list of database keys to delete
        :type ids: list
        :return: True if all the ids were deleted successfully
        :rtype: bool
        """
        try:
            with self._env.begin(write=True) as txn:
                for _id in ids:
                    key = _id.encode()
                    doc = loads(bytes(txn.get(key, db=self._db)))
                    txn.delete(key, db=self._db)
                    for name in self._indexes:
                        self._indexes[name].delete(txn, key, doc)
        except Exception:
            print("~~ exception :: transaction aborted ~~")
            print_stack()
            txn.abort()

    def find(self, index_name=None, max=None):
        """Find all records either sequentiall or based on an index
        
        :param index_name: The name of the index to use [OR use natural order] 
        :type index_name: str
        :param max: The maximum number of records to return
        :type max: int
        :return: The records that were located
        :rtype: list
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
        """Recover a list of indexes for this table

        :return: The indexes for this table
        :rtype: list
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


def debug(self, msg):
    """Display a debug message with current line number and function name

    :param self: A reference to the object calling this routine
    :type self: object
    :param msg: The message you wish to display
    :type msg: str
    """
    if not self._debug: return
    line = _getframe(1).f_lineno
    name = _getframe(1).f_code.co_name
    print("{}: #{} - {}".format(name, line, msg))


def anonymous(text):
    """An anonymous function used to generate functions for database indecies

    :param text: The body of the function call to generate
    :type text: str
    """
    scope = {}
    exec('def func{0}'.format(text), scope)
    return scope['func']


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

