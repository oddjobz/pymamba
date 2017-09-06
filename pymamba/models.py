from json import loads

class BaseModel(object):

    _calculated = {}
    _table = None
    _display = {}
    _doc = {}

    def __init__(self, *args, **kwargs):
        """
        Create a model object based on a supplied dict object
        """
        if len(args):
            self._doc = args[0]

        if 'table' in kwargs:
            self.__class__._table = kwargs['table']


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

        if key in ['_doc', '_table']:
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
        user = self.__class__(loads(json))
        user.validate()
        user._table.append(user._doc)

    def modify(self, uuid, keyval):
        """
        Simple single attribute modification routine
        :param uuid: key for record
        :param keyval: key=val string
        """
        user = self.get(uuid.encode())
        setattr(user, *keyval.split('='))
        user.save()

    def find(self):
        """
        Facilitate a sequential search of the database
        :return: the next record (as a Model)
        """
        for doc in self.__class__._table.find():
            yield self.__class__(doc)

    def list(self, *uuids):
        """
        Generic boxed listing routine
        :param uuid: the options uuid(s) to display
        :return:
        """
        fields = []
        format_line = '+'
        format_head = '| '
        format_data = '| '
        format_spcs = []
        for field in self._display:
            name = field.get('name', None)
            width = field.get('width', None)
            precision = field.get('precision', None)
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
                print(format_data.format(d=doc))
        else:
            if type(uuids) == str:
                uuids = [uuids]
            for uuid in uuids:
                doc = self.get(uuid.encode())
                print(format_data.format(d=doc))
        print(line)
