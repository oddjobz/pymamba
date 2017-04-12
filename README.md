# PyMamba

[![Build Status](https://travis-ci.org/oddjobz/pymamba.svg?branch=master&v=17)](https://travis-ci.org/oddjobz/pymamba)
[![Coverage Status](https://coveralls.io/repos/github/oddjobz/pymamba/badge.svg?branch=master&v=17)](https://coveralls.io/github/oddjobz/pymamba?branch=master)
[![PyPI version](https://badge.fury.io/py/pymamba.svg)](https://badge.fury.io/py/pymamba)
[![PyPI](https://img.shields.io/pypi/dd/pymamba.svg?style=flat)](https://pypi.python.org/pypi/pymamba)
[![Donate](https://img.shields.io/badge/Donate-PayPal-green.svg)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=Y8KQE7TRJV6ZA)

PyMamba is a Python library designed to turn the LMDB storage engine into a relatively 
complete Database package for use in Python3 applications. In the spirit of the 
language title, this package is named after the fastest snake on the planet 
(*Black Mamba*) as a nod to the speed of the LMDB storage engine we're leaning on.

The logic behind a *Python* only database is simply that most modern applications 
tend to be written in higher level languages such as Python, and access speeds when 
measured from the likes of 'C' are in context irrelevant. The driving force behind 
this development was my conclusion that using MySQL for web based applications is 
outdated, and the performance of my preferred alternative (Mongo) is really pretty 
poor. When I stopped to think in more depth, the Python MySQL interface leaves things 
to be desired, and the Mongo interface is primarily JS orientated rather than Pythonic,
so definitely a gap in the market so to speak.

Anyway, without wanting to bore you, PyMamba should be noticeably faster than Mongo, 
and considerably ligther in terms of resource usage. (*so by implication far easier 
to deploy and maintain*)

###### Useful links to other documentation:
* [Sphinx API documentation](https://pymamba.linux.co.uk)
* [A hisory of all Changes](https://oddjobz.github.io/pymamba/CHANGELOG.html)
* [PyMamba on 'Github pages'](https://oddjobz.github.io/pymamba)

### Features

* LMDB mmapped storage engine, arguably the *best* solution for small to medium DB's
* Data is mapped directly to Python objects (ujson) so you read and write *dict's*
* Transparent indexes are keyed by Python 'expression'
* There is no schema, DDL, query language, just *Python*
* Everything is transactional with ACI[D] by default
* LMDB provides multi-thread and multi-process access which defeats GIL limitations

### Examples

This is an example of how to create a new database called *my-database*, then within 
that database to create a table called *people*, then to add some people. 
(this is all *from* the Python shell)

```python
from pymamba import Database
db = Database('my-database')
people = db.table('people')
people.append({'name': 'Fred Bloggs', 'age': 21})
people.append({'name': 'Joe Smith', 'age': 22})
people.append({'name': 'John Doe', 'age': 19})
```
Now there are lots of different ways of recovering information from the database, 
the simplest is just to use **find()** which can be used to scan through the entire 
table. As find returns a generator, you can either use it within a for-loop, or use 
*list* to recover the results as a single list object.

```python
>>> list(people.find())
[{'_id': b'58ed69161839fc5e5a57bc35', 'name': 'Fred Bloggs', 'age': 21}, {'_id': b'58ed69211839fc5e5a57bc36', 'name': 'Joe Smith', 'age': 22}, {'_id': b'58ed69301839fc5e5a57bc37', 'name': 'John Doe', 'age': 19}]

>>> for doc in people.find():
...     print(doc)
... 
{'_id': b'58ed69161839fc5e5a57bc35', 'name': 'Fred Bloggs', 'age': 21}
{'_id': b'58ed69211839fc5e5a57bc36', 'name': 'Joe Smith', 'age': 22}
{'_id': b'58ed69301839fc5e5a57bc37', 'name': 'John Doe', 'age': 19}

```
Note that the returned record includes an *_id* field, this is almost identical to 
the ObjectId field used by Mongo, except we're returning a simple byte-string rather 
than an ObjectId class. A nice feature of dealing with data in this form when matched 
with Python's new 'format' function is the ability to easily format this data like so;

```python
>>> for doc in people.find():
...     print('Name: {name:20} Age:{age:3}'.format(**doc))
... 
Name: Fred Bloggs          Age: 21
Name: Joe Smith            Age: 22
Name: John Doe             Age: 19
```
Or if we just want a subset of the data, we can use an anonymous function to filter our results;
(*note that this is a linear / sequential scan with a filter*)
```python
>>> for doc in people.find(expression=lambda doc: doc['age'] > 21):
...     print('Name: {name:20} Age:{age:3}'.format(**doc))
... 
Name: Joe Smith            Age: 22

```


### Indexing
Transparent indexes are a key part of any database system, and I struggled for a 
while trying to decide which mechanism to use. On the one hand I wanted the 
functionality of being able to index tables by compound fields and functions, and on 
the other I just wanted to be able to simply index based on a single clean field. 
In the end I settled on the following;

```python
>>> people.index('by_name', '{name}')
>>> people.index('by_age_name', '{age:03}{name}')
```
If you're really familiar with Python format strings, you're going to see fairly 
quickly what's going on here, essentially we're indexing by expression only, but 
the expression comes from a Python format string when supplied with the record in 
*dict* format. So you can't directly use a function to do *anything* with regards 
to key generation, but you can do an awful lot with the 
Python [format *mini-language*](https://docs.python.org/3.4/library/string.html#formatspec). 
(and adding actual functions is relatively easy for anyone who can think of a must-have use-case)

So, once we have an index we can search using the index and also find records in 
order based on the index, so we can re-use *find* but this time give it an index to use;
```python
>>> for doc in people.find('by_age_name'):
...     print('Name: {name:20} Age:{age:3}'.format(**doc))
... 
Name: John Doe             Age: 19
Name: Fred Bloggs          Age: 21
Name: Joe Smith            Age: 22
```
Or we can look for specific records;
```python
>>> people.seek_one('by_name', {'name': 'Joe Smith'})
{'_id': b'58ed69211839fc5e5a57bc36', 'name': 'Joe Smith', 'age': 22}
```
Or we can look for a range of records;
```python
>>> for doc in people.range('by_name', {'name': 'J'}, {'name': 'K'}):
...     print('Name: {name:20} Age:{age:3}'.format(**doc))
... 
Name: Joe Smith            Age: 22
Name: John Doe             Age: 19
```
### Updating Records
We've already covered adding new records to the database, so that leaves us with
updating and deleting records. How about this;
```python
>>> person = people.seek_one('by_name', {'name': 'Joe Smith'})
>>> person['age'] += 1
>>> people.save(person)
>>> people.seek_one('by_name', {'name': 'Joe Smith'})
{'_id': b'58ed69211839fc5e5a57bc36', 'name': 'Joe Smith', 'age': 23}
```
And to delete;
```python
>>> person = people.seek_one('by_name', {'name': 'Fred Bloggs'})
>>> people.delete(person['_id'])
>>> for doc in people.find():
...     print('Name: {name:20} Age:{age:3}'.format(**doc))
... 
Name: Joe Smith            Age: 23
Name: John Doe             Age: 19
>>> 
```
There's a lot more to come, but so far it's looking pretty promising.
On my workstation a for-loop based on a **find** yields around **200k** results per second, and an **append** yields around 30k new items per second. This seems to be fairly respectable for a high level language database and seems to be much faster than Mongo when used with either Python or Node.

[![paypal](https://www.paypalobjects.com/en_US/i/btn/btn_donateCC_LG.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_s-xclick&hosted_button_id=Y8KQE7TRJV6ZA)