#### Version 0.1.14

* Switching to Pypi
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
  
