#### Version 0.11

* Re-implemented 'find' method for Table
  * Now returns a generator rather than a list
  * Also accepts a filter, optionally applied after the index
  
* Implemented reindex method for Index class.
  Indexing a table containing data will create an initial index from this data.
  
