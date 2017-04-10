#### Version 0.12

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

#### Version 0.11

* Re-implemented 'find' method for Table
  * Now returns a generator rather than a list
  * Also accepts a filter, optionally applied after the index
  
* Implemented reindex method for Index class.
  Indexing a table containing data will create an initial index from this data.
  
