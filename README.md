# PyMamba

[![Build Status](https://travis-ci.org/oddjobz/pymamba.svg?branch=master&v=14)](https://travis-ci.org/oddjobz/pymamba)
[![Coverage Status](https://coveralls.io/repos/github/oddjobz/pymamba/badge.svg?branch=master&v=14)](https://coveralls.io/github/oddjobz/pymamba?branch=master)

PyMamba is a Python database library which utilises LMDB as a storage engine. It's name is derived from the
*Black Mamba*, the fastest snake on the planet. Typically PyMamba is a number of orders of magnitude
faster than alternatives such as MySQL or MongoDB, the trade-off is a reduced feature-set and a lack of language
portability.

After experimenting with readthedocs.org I've settled on self-hosted Sphinx API documentation, this may change
if issues with RTD can be resolved.

See the API here: [https://pymamba.linux.co.uk](https://pymamba.linux.co.uk)

#### Examples

```python
#!/usr/bin/python3
from mamba import Database, _debug
#
print(">> Define some arbitrary data")
data = [
    {'name': 'Gareth Bult', 'age': 21},
    {'name': 'Squizzey', 'age': 3000},
    {'name': 'Fred Bloggs', 'age': 45},
    {'name': 'John Doe', 'age': 0},
    {'name': 'John Smith', 'age': 40},
]

db = Database("MyDatabase")  # Open (/create) a database
table = db.table('people')   # Open (/create) a table

print('>> Index table by name and age')
table.index('by_name', '{name}')
table.index('by_age', '{age:03}', integer=True, duplicates=True)

print('>> Adding data')
for item in data:
    table.append(item)
print("Count=", table.records)

print('>> Scanning table sequentially')
for record in table.find():
    print('{name} is {age} years old'.format(**record))

print('>> Scanning tables in name order [string index]')
for record in table.find('by_name'):
    print('{name} sorted alphabetically'.format(**record))

print('>> Scanning table in age order [numerical index]')
for record in table.find('by_age'):
    print('{age} - {name} in ascending order of age'.format(**record))

print('>> Scanning on name index with filter')
for record in table.find('by_name', expression=lambda doc: doc['age'] > 40):
    print('{name} is {age} years old (filtered age>40)'.format(**record))

table.drop(True)
db.close()
```

#### Example output

```bash
>> Define some arbitrary data
>> Index table by name and age
>> Adding data
Count= 5
>> Scanning table sequentially
Gareth Bult is 21 years old
Squizzey is 3000 years old
Fred Bloggs is 45 years old
John Doe is 0 years old
John Smith is 40 years old
>> Scanning tables in name order [string index]
Fred Bloggs sorted alphabetically
Gareth Bult sorted alphabetically
John Doe sorted alphabetically
John Smith sorted alphabetically
Squizzey sorted alphabetically
>> Scanning table in age order [numerical index]
0 - John Doe in ascending order of age
3000 - Squizzey in ascending order of age
40 - John Smith in ascending order of age
21 - Gareth Bult in ascending order of age
45 - Fred Bloggs in ascending order of age
>> Scanning on name index with filter
Fred Bloggs is 45 years old (filtered age>40)
Squizzey is 3000 years old (filtered age>40)
```
