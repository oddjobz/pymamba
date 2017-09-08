from datetime import datetime
from copy import  deepcopy

class BaseType(object):
    """
    Base class for all calculated fields
    """
    _vnam = None

    def __init__(self, vnam='undef'):
        """
        Create a new custom field
        :param vnam: name of field
        """
        self._vnam = vnam

    @property
    def name(self):
        """
        :return: the name of the field (string)
        """
        return self._vnam

    def from_internal(self, doc):
        """
        Return the field in 'external' format
        :param doc: the current record
        :return: the field in external format
        """
        return doc.get(self._vnam, None)

    def to_internal(self, doc, value):
        """
        Return the field in internal format
        :param value: the field in external format
        :return: the field in internal format
        """
        doc[self._vnam] = value


class DateType(BaseType):

    _format = None

    def __init__(self, vnam, fmt='%d/%m/%Y'):
        """
        Set the date format
        :param format: valid date format string
        """
        super().__init__(vnam)
        self._format = fmt

    def from_internal(self, doc):
        """
        Convert from an integer (internal) to a string (external)
        :param doc: current record
        :return: date (string)
        """
        val = doc.get(self._vnam, None)
        return datetime.fromtimestamp(val).strftime(self._format)

    def to_internal(self, doc, value):
        """
        Convert from external string representation to internal int format
        :param value: string date
        :return: integer date
        """
        doc[self._vnam] = datetime.strptime(value, self._format).timestamp()


class AgeType(BaseType):

    def from_internal(self, doc):
        """
        Calculate age based on current time and 'dob' field
        :param doc: the current record
        :return: age (integer)
        """
        now = datetime.now()
        val = doc.get(self._vnam, now)
        dob = datetime.fromtimestamp(val)
        return (now - dob).days // 365


class NameType(BaseType):

    def from_internal(self, doc):
        """
        Return a 'full name' as a concatenation of fields
        :param doc: the current record
        :return: name (string)
        """
        name = ''
        for field in self._vnam:
            if len(name):
                name += ' '
            name += doc.get(field, '')
        return name


class UUIDType(BaseType):

    def from_internal(self, doc):
        return doc[self._vnam].decode()


class ManyToManyLink(BaseType):

    _table = None
    _target = None

    def __init__(self, table, classA, classB):
        self._table = table
        self._classA = classA
        self._classB = classB
        self._src_key = self._classA._table._name
        self._dst_key = self._classB._table._name
        self._results = None

    def from_internal(self, doc):
        if not self._results:
            self._results = []
            key = {self._src_key: doc['_id'].decode()}
            for link in self._table.seek(self._src_key, key):
                dst_key = link[self._dst_key].encode()
                target = self._classB._table.get(dst_key)
                self._results.append(self._classB.__class__(target, table=self._classB._table))
            self._original = self._results[:]
        return self._results

    def to_internal(self, doc, value):
        #doc[self._vnam] = value
        print("Changing!!!!=>", value)
