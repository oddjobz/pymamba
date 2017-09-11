# PyMamba - ORM

The native PyMamba interface is not unlike Mongo in that it treats each record (or document) 
as a Python dictionary. For databases that involve single / unrelated tables, this is fine 
and the most efficient means to access data. If however you're mapping relationships between
tables, as you might with a traditional SQL database, maintaining linkage tables can be
a bit fiddly, and it you're used to something like SQLAlchemy, the standard interface may
seem a little *raw*.

To this end there we have a built-in mechanism for overlaying some structure onto our raw
tables to give things a bit of an *Alchemy* feel. If you're not used to ORM's then this 
might look a bit like *magic*, but for *SQLAlchemy* users, you should feel right at home
and hopefully wondering why *SQLAlchemy* isn't this easy ... ;-)

#### Current Features

So, what we're catering for at the moment;

* Calculated fields
    * Date
    * Age
    * Name
    * UUID
    * Custom
* ManyToMany links between tables
* Table pretty-printer
* OneToMany links between tables [TODO]
* Referential integrity control [TODO]
* Link attributes [TODO] 

*We do have a little work left to do as you can see, but the heart of the ORM is up and
running and seem to work fairly well*.

#### How to Use Models

The idea is that we wrap each table up in a dedicated class then we can create additional
classes to link the (wrapped) tables together. Here's a very simple example;

```python
from pymamba import Database
from pymamba.models import BaseModel
from pymamba.types import AgeType

class UserModel(BaseModel):
    _calculated = {
        'age': AgeType('dob')
    }
    _display = [
        {'name': 'forename', 'width': 20},
        {'name': 'surname', 'width': 20},
        {'name': 'dob', 'width': 15},
        {'name': 'age', 'width': 3}
    ]
    
db = Database('my_db', {'env': {'map_size': 1024 * 1024 * 10}})
user_model = UserModel(table=db.table('users'))
```
If you save this to a file (demo.py) you should then be able to do the following;
```python
>>> from demo import user_model
>>> import datetime
>>> user_model.append({'forename':'fred','surname':'bloggs','dob':datetime.date(1970,12,1)})
>>> user_model.list()
+----------------------+----------------------+-----------------+-----+
| forename             | surname              | dob             | age | 
+----------------------+----------------------+-----------------+-----+
| fred                 | bloggs               |        28857600 |  46 | 
+----------------------+----------------------+-----------------+-----+
```
Note that age isn't a stored field, it's generated on the fly from the 'dob' field hence
will dynamically change whenever the dob field is updated. Also, the *list* function is
driven (by default) by the attributes listed in *_display*.

As it stands the date of birth isn't terribly readable, so we could add another field to
the mix to ger around this, in calculated add;
```python
    'birthday': DateType('dob')
```
And change the display section to show *birthday* rather then *dob*, then try the above
operation again and you should get (don't forget to add *DateType* to your imports);
```python
>>> from demo import user_model
>>> user_model.list()
+----------------------+----------------------+-----------------+-----+
| forename             | surname              | birthday        | age | 
+----------------------+----------------------+-----------------+-----+
| fred                 | bloggs               | 01/12/1970      |  46 | 
+----------------------+----------------------+-----------------+-----+
```
So far this all looks relatively trivial, the real value comes in what it's doing under
the hood. Let's try to update this data, take a look at the following;
```python
>>> from demo import user_model
>>> user = list(user_model.find())[0]
>>> user.surname='Bloggs Updated'
>>> user.save()
>>> user_model.list()
+----------------------+----------------------+-----------------+-----+
| forename             | surname              | birthday        | age | 
+----------------------+----------------------+-----------------+-----+
| fred                 | Bloggs Updated       | 01/12/1970      |  46 | 
+----------------------+----------------------+-----------------+-----+
```
The *.find()* method for a model just returns all records (as an array) so all we're doing
here is assigning 'user' to the first record in the table. Each field in the table is 
then accessible as an attribute (i.e. user.forename, user.surename, user.dob etc) which
is a little more natural than updating a *dict*, then *save* updates changes in the model
back to the actual table. Again relatively trivial, however this is quite neat;
```python
>>> print(user.age, user.birthday)
46 01/12/1970
```
i.e. when you access the model, you will see attributes that are generated on the fly in 
additional to any stored data, and (!) if you don't access them they're not generated so
there's no overhead in having *lots* of rarely used calculated fields.

### How to use Relationships

So this is where things get a little more interesting. In standard NoSQL, typically there
is no real concept of table linkage, foreign keys or referential integrity. However, that
doesn't mean the concepts are invalid or no longer needed, so, here is NoSQL with inter-
table relationships, managed by a built-in ORM (!)

First, let's start by defining a second table, we're going to make it really easy by just
having an address table, then working on the premise that users can have multiple 
addresses, and that a number of users can live at each address.

```python
class AddressModel(BaseModel):

    _display = [
        {'name': 'address', 'width': 30},
        {'name': 'postcode', 'width': 15}
    ]
``` 
And we will create a relationship between the UserModel and the AddressModel by adding
this to our previous code;
```python
address_model = AddressModel(table=db.table('addresses'))
links = ManyToMany(db, user_model, address_model)
```
So, starting up as before we can do this;
```python
from demo import user_model, address_model, UserModel
import datetime
>>> user = UserModel({'forename':'john','surname':'smith','dob':datetime.date(1971,12,1)})
>>> user.addresses.append({'address': 'address1', 'postcode': 'postcode1'})
>>> user.addresses.append({'address': 'address2', 'postcode': 'postcode2'})
>>> user_model.append(user)
>>> user_model.list()
+----------------------+----------------------+-----------------+-----+
| forename             | surname              | birthday        | age | 
+----------------------+----------------------+-----------------+-----+
| john                 | smith                | 01/12/1971      |  45 | 
+----------------------+----------------------+-----------------+-----+
>>> address_model.list()
+--------------------------------+-----------------+
| address                        | postcode        | 
+--------------------------------+-----------------+
| address1                       | postcode1       | 
| address2                       | postcode2       | 
+--------------------------------+-----------------+
```
So there are some interesting things going on here, we have created a new instance of *UserModel*, then 
added two new addresses by appending to it's *address* property. Now the address property is a virtual
field created by the "ManyToMany" link and not only is it populated from the *address* table, but it can
also be used to append, update and delete entries in the address table. On further inspection we see;
```python
>>> user
{'surname': 'smith', '_id': b'59b6860b1839fc4ee8c00596', 'forename': 'john', 'dob': datetime.date(1971, 12, 1)}
>>> user.addresses
[{'address': 'address1', 'postcode': 'postcode1', '_id': b'59b6860b1839fc4ee8c00597'}, {'address': 'address2', 'postcode': 'postcode2', '_id': b'59b6860b1839fc4ee8c00599'}]
>>> type(user.addresses[0])
<class 'demo.AddressModel'>
```
Again, virtual and calculated fields are only evaluated when reading through the users table, the cost of
reading associated tables is only incurred if the linked attributes (addresses in this case) are accessed.
Note that the *addresses* field is a list, but of type *AddressModel*, rather than of a raw *dict*.

##### Updating linkes tables

In a similar fashion, we can do updates to the linked table;
```python
>>> user = list(user_model.find())[0]
>>> user
{'surname': 'smith', '_id': b'59b6860b1839fc4ee8c00596', 'forename': 'john', 'dob': 60393600}
>>> user.addresses[1]
{'address': 'address2', 'postcode': 'postcode2', '_id': b'59b6860b1839fc4ee8c00599'}
>>> user.addresses[1].postcode = 'A new postcode'
>>> user.save()
>>> address_model.list()
+--------------------------------+-----------------+
| address                        | postcode        | 
+--------------------------------+-----------------+
| address1                       | postcode1       | 
| address2                       | A new postcode  | 
+--------------------------------+-----------------+
```

##### Deleting entries in linked tables

And of course, we can delete in the same way, but be aware that this will only sever the link rather than
deleting the address, so future references to addresses in this example will only show the user linked to
one address, but a listing of the address table will show both addresses. Deleting target objects with a 
zero reference count will be an option when the referential integrity code is added.
```python
>>> del user.addresses[0]
>>> user.save()
>>> address_model.list()
>>> user = list(user_model.find())[0]
>>> user.addresses
[{'address': 'address2', 'postcode': 'A new postcode', '_id': b'59b6860b1839fc4ee8c00599'}]
```
If we wanted to re-instate the relationship in this instance we could do;
```python

```