from json import loads
from .types import ManyToManyLink


class BaseModel(object):

    _calculated = {}
    _display = {}

    def __init__(self, *args, **kwargs):
        """
        Create a model object based on a supplied dict object
        """
        self._doc = args[0] if len(args) else {}
        if 'table' in kwargs:
            self._table = kwargs['table']

        self._dirty = False

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
        if key in ['_table', '_links', '_dirty']:
            super().__setattr__(key, value)
        elif key == '_doc':
            super().__setattr__(key, value)
            self._dirty = True
        elif key not in self._calculated:
            self._doc[key] = value
            self._dirty = True
        else:
            self._calculated[key].to_internal(self._doc, value)
            self._dirty = True

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

    def get(self, key):
        """
        Get a record from the database using it's UUID
        :param key: uuid (primary key)
        :return: record (as a Model)
        """
        return self.__class__(self._table.get(key)).format()

    def modify(self, uuid, keyval):
        """
        Simple single attribute modification routine
        :param uuid: key for record
        :param keyval: key=val string
        """
        setattr(self, *keyval.split('='))
        self.save()

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
            functions.append(func) if func else None
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
                for func in functions:
                    fn = getattr(doc, func, None)
                    fn() if fn else None
                print(format_data.format(d=doc))
        else:
            for uuid in uuids:
                doc = self.get(uuid.encode())
                print(format_data.format(d=doc))
        print(line)

    def add_link(self, link, doc, context):
        """
        Add an entry to the link table
        :param link: link definition
        :param doc: the document containing the relevant details
        :param context: the document to link to
        :return:
        """
        lhs = link._classB._table._name
        rhs = link._classA._table._name
        linkage = {lhs: doc._id.decode(), rhs: context._id.decode()}
        link._table.append(linkage)

    def add_dependent(self, link, doc, context):
        """
        Add a new dependent item
        :param link:
        :param doc:
        :return:
        """
        link._classB._table.append(doc._doc)
        self.add_link(link, doc, context)

    def upd_dependent(self, link, doc, context):
        """
        Update a dependent record
        :param link:
        :param doc:
        :return:
        """
        lhs = link._classB._table._name
        rhs = link._classA._table._name
        if doc._dirty:
            doc.validate()
            link._classB._table.save(doc._doc)
            doc._dirty = False
        linkage = {lhs: doc._id.decode(), rhs: context._id.decode()}
        item = link._table.seek_one(lhs, linkage)
        self.add_link(link, doc, context) if not item else None

    def del_dependent(self, link, doc, context):
        """
        Delete a link to a record in a dependent table
        :param link: the link table
        :param doc: the document to unlink
        :return:
        """
        item = link._table.seek_one(doc._table._name, {doc._table._name:doc._id.decode()})
        if not item: raise xForeignKeyViolation('link table item is missing')
        link._table.delete(item['_id'])

    def add(self, model):
        """
        Append the current record to the Database
        """
        if type(model) is str:
            model = loads(model)
        if type(model) is dict:
            model = self.__class__(model)
        model.validate()
        self._table.append(model._doc)
        model._dirty = False
        self.update_links(model)

    def save(self):
        """
        Save this record in the database
        """
        if self._dirty:
            self.validate()
            self._table.save(self._doc)
            self._dirty = False
        self.update_links(self)

    def update_links(self, context):
        #
        #   Process any changes to dependencies
        #
        for link in context._links:
            if link._results:
                for i, doc in enumerate(link._results):
                    if type(doc) == dict:
                        doc = link._classB.__class__(doc, table=link._classB._table)
                        link._results[i] = doc
                    if not doc._id:
                        self.add_dependent(link, doc, context)
                    elif doc._dirty:
                        self.upd_dependent(link, doc, context)
                for doc in set(link._original)-set(link._results):
                    self.del_dependent(link, doc, context)
            link._results = None


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


class xForeignKeyViolation(BaseException):
    """Foreign key entry is missing"""

