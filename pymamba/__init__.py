"""
PyMamba is a database schema that sits on top of the LMDB storage engine. It provides a
higher level of functionality than is provided by the RAW LMDB API, primarily the ability
to transparently handle indexes, and the ability to transparently read and write Python
variables (including list/dict structures). Currently it's *much* faster than Mongo, but 
not feature complete and not exhaustively tested in the real world.
"""
##############################################################################
# TODO Items go here ...
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
from sys import _getframe, maxsize
from uuid import uuid1 as uuid

__version__ = "0.1.14"

class Database(object):
    """
    Representation of a Database, this is the main API class
    
    :param name: The name of the database to open
    :type name: str
    :param conf: Any additional or custom options for this environment
    :type conf: dict  
    """
    _debug = False
    _conf = {
        'map_size': 1024*1024*1024*2,
        'subdir': True,
        'metasync': False,
        'sync': True,
        'lock': True,
        'max_dbs': 64,
        'writemap': True,
        'map_async': True
    }

    def __init__(self, name, conf=None):
        conf = dict(self._conf, **conf.get('env', {})) if conf else self._conf
        self._tables = {}
        self._env = Environment(name, **conf)
        self._db = self._env.open_db()

    def __del__(self):
        self.close()

    def close(self):
        """
        Close the current database
        """
        if self._env:
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
    Mapping for table indecies, this is version #2 with a much simplified indexing scheme.
    
    :param env: An LMDB Environment object
    :type env: Environment
    :param name: The name of the index we're working with
    :type name: str
    :param func: Is a Python format string that specified the index layout
    :type func: str
    :param conf: Configuration options for this index
    :type conf: dict

    """
    _debug = False

    def __init__(self, env, name, func, conf):
        self._env = env
        self._name = name
        self._conf = conf
        self._conf['key'] = self._conf['key'].encode()
        self._func = _anonymous('(r): return "{}".format(**r).encode()'.format(func))
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

    def set_key(self, cursor, record):
        """
        Set the cursor to the first matching record
        
        :param cursor: An active cursor 
        :type cursor: Cursor
        :param record: A template record specifying the key to use
        :type record: dict
        """
        cursor.set_key(self._func(record))

    def set_range(self, cursor, record):
        """
        Set the cursor to the first matching record

        :param cursor: An active cursor 
        :type cursor: Cursor
        :param record: A template record specifying the key to use
        :type record: dict
        """
        cursor.set_range(self._func(record))

    def set_next(self, cursor, record):
        """
        Find the next matching record lower than the key specified

        :param cursor: An active cursor 
        :type cursor: Cursor
        :param record: A template record specifying the key to use
        :type record: dict
        """
        cursor.next()
        return cursor.key() <= self._func(record)

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

    def empty(self, txn):
        """
        Empty the current index of all records
        
        :param txn: Is an open Transaction 
        """
        return txn.drop(self._db, delete=False)

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
        try:
            ikey = self._func(record)
            return txn.put(ikey, key.encode(), db=self._db)
        except KeyError as error:
            return False

    def save(self, txn, key, old, rec):
        """
        Save any changes to the keys for this record
        
        :param txn: An active transaction
        :type txn: Transaction
        :param key: The key for the record in question
        :type key: str
        :param old: The record in it's previous state 
        :type old: dict
        :param rec: The record in it's amended state
        :type rec: dict
        """
        old_key = self._func(old)
        new_key = self._func(rec)
        if old_key != new_key:
            txn.delete(old_key, key, db=self._db)
            txn.put(new_key, key, db=self._db)

    def reindex(self, db, txn=None):
        """
        Reindex the current index, rec
         
        :param db: A handle to the database table to index
        :type db: database handle
        :param txn: An open transaction
        :type txn: Transaction
        :return: Number of index entries created
        :rtype: int
        """
        def worker():
            count = 0
            self.empty(txn)
            try:
                with Cursor(db, txn) as cursor:
                    if cursor.first():
                        while True:
                            record = loads(bytes(cursor.value()))
                            if self.put(txn, record['_id'], record):
                                count += 1
                            if not cursor.next():
                                break
            except Exception as error:
                txn.abort()
                raise error
            return count

        if txn:
            return worker()
        with self._env.begin(write=True) as txn:
            return worker()


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

    def append(self, record, txn=None):
        """
        Append a new record to this table
        
        :param record: The record to append
        :type record: dict
        :param txn: An open transaction
        :type txn: Transaction
        """
        def worker():
            try:
                key = str(uuid())
                record['_id'] = key
                txn.put(key.encode(), dumps(record).encode(), db=self._db, append=True)
                for name in self._indexes:
                    self._indexes[name].put(txn, key, record)

            except Exception as error:
                txn.abort()
                raise error

        if txn:
            worker()
        else:
            with self._env.begin(write=True) as txn:
                worker()

    def delete(self, keys):
        """
        Delete a record from this table
        
        :param keys: A list of database keys to delete
        :type keys: list
        """
        if not isinstance(keys, list):
            keys = [keys]
        with self._env.begin(write=True) as txn:
            try:
                for _id in keys:
                    key = _id.encode()
                    doc = loads(bytes(txn.get(key, db=self._db)))
                    txn.delete(key, db=self._db)
                    for name in self._indexes:
                        self._indexes[name].delete(txn, key, doc)
            except Exception as error:
                txn.abort()
                raise error

    def drop(self, delete=True):
        """
        Drop this tablex and all it's indecies

        :param delete: Whether we delete the table after removing all items
        :type delete: bool
        """
        with self._env.begin(write=True) as txn:
            try:
                for name in self.indexes:
                    if delete:
                        self.unindex(name, txn)
                    else:
                        self._indexes[name].empty(txn)
                txn.drop(self._db, delete)
            except Exception as error:
                txn.abort()
                raise error

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

    def find(self, index=None, expression=None, limit=maxsize):
        """
        Find all records either sequential or based on an index

        :param index: The name of the index to use [OR use natural order] 
        :type index: str
        :param expression: An optional filter expression
        :type expression: function
        :param limit: The maximum number of records to return
        :type limit: int
        :return: The next record (generator)
        :rtype: dict
        """
        with self._env.begin() as txn:
            if not index:
                cursor = Cursor(self._db, txn)
            else:
                if index not in self._indexes:
                    raise xIndexMissing(index)
                index = self._indexes[index]
                cursor = index.cursor(txn)
            count = 0
            first = True
            while count < limit:
                if not (cursor.first() if first else cursor.next()):
                    break
                first = False
                record = cursor.value()
                if index:
                    record = txn.get(record, db=self._db)
                record = loads(bytes(record))
                if callable(expression) and not expression(record):
                    continue
                yield record
                count += 1

            cursor.close()

    def get(self, key):
        """
        Get a single record based on it's key
        
        :param key: The _id of the record to get
        :type key: str
        :return: The requested record
        :rtype: dict
        """
        with self._env.begin() as txn:
            return loads(bytes(txn.get(key, db=self._db)))

    def index(self, name, func=None, duplicates=False):
        """
        Return a reference for a names index, or create if not available

        :param name: The name of the index to create
        :type name: str
        :param func: A specification of the index, !<function>|<field name>
        :type func: str
        :param duplicates: Whether this index will allow duplicate keys
        :type duplicates: bool
        :return: A reference to the index, created index, or None if index creation fails
        :rtype: Index
        :raises: lmdb_Aborted on error
        """
        if name not in self._indexes:
            conf = {
                'key': _index_name(self, name),
                'dupsort': duplicates,
                'create': True,
            }
            self._indexes[name] = Index(self._env, name, func, conf)
            with self._env.begin(write=True) as txn:
                key = ''.join(['@', _index_name(self, name)]).encode()
                val = dumps({'conf': conf, 'func': func}).encode()
                txn.put(key, val)
                self._indexes[name].reindex(self._db, txn)

        return self._indexes[name]

    def save(self, record, txn=None):
        """
        Save an changes to a pre-existing record

        :param record: The record to be saved
        :type record: dict
        :param txn: An open transaction
        :type txn: Transaction
        """

        def worker():
            key = record['_id'].encode()
            old = loads(bytes(txn.get(key, db=self._db)))
            txn.put(key, dumps(record).encode(), db=self._db)
            for name in self._indexes:
                self._indexes[name].save(txn, key, old, record)

        if txn: worker()
        else:
            with self._env.begin(write=True) as txn:
                worker()

    def seek(self, index, record):
        """
        Find all records matching the key in the specified index.
        
        :param index: Name of the index to seek on
        :type index: str
        :param record: A template record containing the fields to search on
        :type record: dict
        :return: The records with matching keys (generator)
        :type: dict
        """
        with self._env.begin() as txn:
            index = self._indexes[index]
            with index.cursor(txn) as cursor:
                index.set_key(cursor, record)
                while True:
                    if not cursor.key():
                        break
                    record = txn.get(cursor.value(), db=self._db)
                    yield loads(bytes(record))
                    if not cursor.next_dup():
                        break

    def range(self, index, lower, upper):
        """
        Find all records with a key >= lower and <= upper
        
        :param index: The name of the index to search
        :type index: str
        :param lower: A template record containing the lower end of the range
        :type lower: dict
        :param upper: A template record containing the upper end of the range
        :type upper: dict
        :return: The records with keys witin the specified range (generator)
        :type: dict
        """
        with self._env.begin() as txn:
            index = self._indexes[index]
            with index.cursor(txn) as cursor:
                index.set_range(cursor, lower)
                while True:
                    if not cursor.key(): return
                    record = txn.get(cursor.value(), db=self._db)
                    yield loads(bytes(record))
                    if not index.set_next(cursor, upper):
                        return

    def unindex(self, name, txn=None):
        """
        Delete the named index

        :return: 
        :param name: The name of the index
        :type name: str
        :param txn: An active transaction
        :type txn: Transaction
        :raises: lmdb_IndexMissing if the index does not exist
        """
        if name not in self._indexes:
            raise xIndexMissing()

        def worker():
            try:
                self._indexes[name].drop(txn)
                del self._indexes[name]
                txn.delete(''.join(['@', _index_name(self, name)]).encode())
            except Exception as error:
                txn.abort(); raise error

        if txn:
            worker()
        else:
            with self._env.begin(write=True) as txn:
                worker()

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
    if hasattr(self, '_debug') and self._debug:
        line = _getframe(1).f_lineno
        name = _getframe(1).f_code.co_name
        print("{}: #{} - {}".format(name, line, msg))


def _anonymous(text):
    """
    An function used to generate anonymous functions for database indecies

    :param text: The body of the function call to generate
    :type text: str
    :return: Anonymous function to calculate key value
    :rtype: function
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


class xTableExists(Exception):
    """Exception - database table already exists"""
    pass


class xIndexExists(Exception):
    """Exception - index already exists"""
    pass


class xTableMissing(Exception):
    """Exception - database table does not exist"""
    pass


class xIndexMissing(Exception):
    """Exception - index does not exist"""
    pass


class xNotFound(Exception):
    """Exception - expected record was not found"""
    pass


class xAborted(Exception):
    """Exception - transaction did not complete"""


class xWriteFail(Exception):
    """Exception - write failed"""
