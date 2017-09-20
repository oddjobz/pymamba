### Version 0.2.9

* Updating README.rst

### Version 0.2.8

* Changed requirements, backed out of previous change

### Version 0.2.7

* Changed requirements, replaced pymongo with bson

### Version 0.2.6

* Additional unit testing (address_book)
* some bug fixes in relation to address_book

### Version 0.2.5

* Couple of critical bug fixes in the new ORM code
* Couple of related fixes to the addressbook demo
* A little refactoring

### Version 0.2.4

* Adding addressbook demo (nested relationships)
* A number of related bugfixes

### Version 0.2

* Complete restructure to allow for application level transactions (for replication)
* Privatised some methods that don't need to be exposed
* Database now has a **.binlog(bool)** function (logging off by default)
* Database now has **.begin()** and **.end()** methods (begin supports **with**)
* All **Table** functions should now be wrapped to ensure they're included in repl transactions
* ORM - we now have a built-in (optional) ORM with support for custom models and custom types
* ORM - includes support for calculated (virtual) fields and transparent many-to-many links
* Added README-ORM
* Main test suite was added to and transaction testing added
* Two new suites were added for model and ORM testing
* Tidied up some of the examples and benchmark
* Updated README to include link to ORM docs and sample benchmark results

### Version 0.1.35

* Fixed bug in 'seek' where the index key rather than the primary key was returned in the record body
* Clean up exception handling on append
* Fix append to allow the use of user-generated _id's
* Facilitate environment-wide txn

### Version 0.1.32

* Updated range code to include None as a possible upper and/or lower limit

### Version 0.1.31

* Added three new test cases
* Added "inclusive" option to table.range, set False to exclude matching keys at 
each end of the range results.

### Version 0.1.30

* _id no longer stored in record body (removes duplication)
* Removes unnecessary decode in range
* Lots of misc bug fixes and exception fixes
* New Readme
* Added seek_one function
* Switched from UUID to bson.**ObjectId**

#### Version 0.1.14

* Switching to PyPi
* You can now download this package with "pip", note however it will only work with Python3 for now.

#### Version 0.1.4

* Fixed bug in empty to stop it deleting indecies
* Added 'save' method to table for updating pre-existing items (index aware)
* Added partial indexes, so only items where the key is not null are included in the index
* Code coverage hits 100%

#### Version 0.1.3

* Added seek(index, record) - seek to the first matching record based on an index key

  For example;
```  
table.index('by_compound', '{cat}|{name}')
for doc in table.seek('by_compound', {'cat': 'A', 'name': 'Squizzey'}):
    print(doc)
```

* Added range(index, lower, upper) - return all records with keys falling within the
  limits set up (upper, lower)

  For example;
  
```  
table.index('by_compound', '{cat}|{name}')
for doc in table.range('by_compound', {'cat': 'A', 'name': 'Squizzey'}, {'cat': 'B', 'name': 'Gareth Bult1'):
    print(doc)
```

#### Version 0.1.2

* Simplified the way indexes work, maintaining a second 'easy' method is no
  longer worthwhile with the new indexing interface. All indexes are now specified
  using format string, so a simple index on a field called name is specified as;
  
  - {name}
  
  A compound index based on name and age would be;
  
  - {name}{age}
  
  Appling a little logic, if you want records in age order, and age is always in
  the range 0-999 and is sorted numerically, we want the index specification to be;
  
  - {age:03}{name}
  
  i.e. a numberic, formatted as a string, length 3, zero padded
  
  If we wanted sorting by name, we would probably want;
  
  - {name}|{age:03}
  
  Not the use of a sepatating character (|) to ensure separation between the variable 
  length alphanumeric key and the fixed length numeric key.

#### Version 0.1.1

* Re-implemented 'find' method for Table
  * Now returns a generator rather than a list
  * Also accepts a filter, optionally applied after the index
  
* Implemented reindex method for Index class.
  Indexing a table containing data will create an initial index from this data.
  
