from json import loads
from .types import ManyToManyLink


class BaseModel(object):

    def __init__(self, *args, **kwargs):
        """
        Create a model object based on a supplied dict object
        """
        if len(args):
            self._doc = args[0]

        if 'table' in kwargs:
            self._table = kwargs['table']

    def __getattr__(self, key):
        """
        Get the value of a real or calculated field
        :param key: field name
        :return: calculated field value
        """
        if key in self._calculated:
            return self._calculated[key].from_internal(self._doc)
        if key not in self._doc:
            return ''
        return self._doc[key]

    def __setattr__(self, key, value):
        """
        Override the default set for local calculated fields
        :param key: the attribute name
        :param value: the new value for the attribute
        """

        if key in ['_doc', '_table', '_links']:
            super().__setattr__(key, value)
        else:
            if key not in self._calculated:
                self._doc[key] = value
            else:
                self._calculated[key].to_internal(self._doc, value)

    def __repr__(self):
        """
        Default display string
        :return: string representation of the record
        """
        return str(self._doc)

    def validate(self):
        """
        Validate the current record against any available validators
        """
        for link in self._links:
            print("Checking:", link)
            if not link._original or not link._results:
                print("Ignoring no change")
                continue
            print("Deleted: ", set(link._original) - set(link._results))
            print("Added: ", set(link._results) - set(link._original))
            for field in list(self._doc):
                if field in self._calculated:
                    self.__setattr__(field, self._doc[field])
        return self

    def format(self):
        """
        Validate the current record against any available validators
        """
        for field in self._doc:
            if field in self._calculated:
                self._doc[field] = self.__getattr__(field)
        return self

    def save(self):
        """
        Save this record in the database
        """
        self.validate()
        self._table.save(self._doc)

    def get(self, key):
        """
        Get a record from the database using it's UUID
        :param key: uuid (primary key)
        :return: record (as a Model)
        """
        return self.__class__(self._table.get(key)).format()

    def append(self, json):
        """
        Append the current record to the Database
        """
        model = self.__class__(loads(json))
        model.validate()
        self._table.append(model._doc)

    def modify(self, uuid, keyval):
        """
        Simple single attribute modification routine
        :param uuid: key for record
        :param keyval: key=val string
        """
        instance = self.get(uuid.encode())
        setattr(instance, *keyval.split('='))
        instance.save()

    def find(self):
        """
        Facilitate a sequential search of the database
        :return: the next record (as a Model)
        """
        for doc in self._table.find():
            yield self.__class__(doc, table=self._table)

    def list(self, *uuids):
        """
        Generic boxed listing routine
        :param uuid: the options uuid(s) to display
        :return:
        """
        fields = []
        functions = []
        format_line = '+'
        format_head = '| '
        format_data = '| '
        format_spcs = []
        for field in self._display:
            name = field.get('name', None)
            width = field.get('width', None)
            precision = field.get('precision', None)
            func = field.get('function', None)
            if func:
                functions.append(func)
            fields.append(name)
            format_line += '{}+'
            format_head += '{{:{}.{}}} | '.format(width, width)
            if precision:
                format_data += '{{d.{}:{}.{}}} | '.format(name, width, precision)
            else:
                format_data += '{{d.{}:{}}} | '.format(name, width)
            format_spcs.append('-'*(width+2))
        line = format_line.format(*format_spcs)
        head = format_head.format(*fields)

        print(line)
        print(head)
        print(line)
        if not uuids:
            for doc in self.find():
                doc.addresses.append(self.__class__({'line1': 'New Address', 'postcode': 'New Postcode'}))
                doc.save()

            for doc in self.find():
                for func in functions:
                    fn = getattr(doc, func, None)
                    fn() if fn else None
                try:
                    print(format_data.format(d=doc))
                except TypeError as e:
                    print("Format>", format_data)
                    print("Doc>", doc)
                    print(e)
        else:
            for uuid in uuids:
                doc = self.get(uuid.encode())
                print(format_data.format(d=doc))
        print(line)


class ManyToMany(object):

    _table = None

    def __init__(self, database, classA, classB):
        table = 'rel_{}_{}'.format(classA._table._name, classB._table._name)
        self._table = database.table(table)
        self._table.index(classA._table._name, '{{{}}}'.format(classA._table._name), duplicates=True)
        self._table.index(classB._table._name, '{{{}}}'.format(classB._table._name), duplicates=True)
        linkA = ManyToManyLink(self._table, classA, classB)
        linkB = ManyToManyLink(self._table, classB, classA)
        classA._calculated[classB._table._name] = linkA
        classB._calculated[classA._table._name] = linkB
        if not hasattr(classA.__class__, "_links"):
            classA.__class__._links = []
        if not hasattr(classB.__class__, "_links"):
            classB.__class__._links = []
        classA._links.append(linkA)
        classB._links.append(linkB)
