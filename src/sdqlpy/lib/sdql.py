class SemiRing:

    def __init__(self):
        pass

    def __eq__(self, other):
        if self is None and other is None:
            return True
        if self is None or other is None:
            return False
        if self.value == other.value:
            return True
        return False

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        isnull_self = self == None
        isnull_other = other == None
        if isnull_self and isnull_other:
            return False
        if isnull_self or isnull_other:
            return False
        if self.value < other.value:
            return True
        return False

    def __le__(self, other):
        if self < other or self == other:
            return True
        return False

    def __gt__(self, other):
        if not(self <= other):
            return True
        return False

    def __ge__(self, other):
        if not(self < other):
            return True
        return False

    def __hash__(self):
        return hash((str(self)))

    def __len__(self):
        if type(self) == SemiRingBool:
            return self.value
        return len(self.value)

    # def __neg__(self):
    #     if type(self) == SemiRingFloat:
    #         return SemiRingFloat(-0.01*(self.value))
    #     elif type(self) == SemiRingInt:
    #         return SemiRingInt(-1*(self.value))

class SemiRingDictionary(SemiRing):

    @property
    def value(self):
        return self.__value

    def __init__(self, key_type=SemiRing, value_type=SemiRing, initialvalue=None):
        SemiRing.__init__(self)
        self.__key_type = key_type
        self.__value_type = value_type
        if initialvalue is not None:
            self.__value = dict(initialvalue)
        else:
            self.__value = dict()

    def get_type(self):
        return (self.__key_type, self.__value_type)

    def __str__(self):
        if (len(self.value.items()) == 0):
            return "{}"
        res = "{ "
        for (k, v) in self.value.items():
            if type(k) == str:
                res += "\"" + k + "\""
            else:
                res += str(k)
            res += " -> "
            if type(v) == str:
                res += "\"" + v + "\""
            else:
                res += str(v)
            res += ", "
        return res[:len(res)-2:]+"}"

    def __add__(self, other):
        if (self.get_type() == (SemiRing, SemiRing)) or (other.get_type() == (SemiRing, SemiRing)) or (self.get_type() == other.get_type()):
            if type(other) == SemiRingDictionary or type(other) == SemiRingSet:
                res = SemiRingDictionary(
                    self.get_type()[0], self.get_type()[1])
                for (k, v) in self.value.items():
                    res[k] = v + \
                        other.value.get(k, zero(v))
                for k in other.value:
                    if k not in res.value:
                        res[k] = other[k] + \
                            zero(other[k])
                return res
            else:
                raise ValueError('SemiRingDictionary + ' +
                                 str(type(other)) + " is not allowed!")
        else:
            raise ValueError("{" + str(self.get_type()[0]) + " -> " + str(self.get_type()[1]) + "} + {" + str(
                other.get_type()[0]) + " -> " + str(other.get_type()[1]) + "} is not supported!")

    def __mul__(self, other):
        res = None
        for (k, v) in self.value.items():
            tmp = v * other
            if res == None:
                res = SemiRingDictionary(self.get_type()[0], type(tmp))
            res[k] = tmp
        if res == None:
            res = SemiRingDictionary(self.get_type()[0], self.get_type()[1])
        return res

    def __getitem__(self, key):
        tmpvalue = self.value.get(key)
        if tmpvalue != None:
            return tmpvalue
        else:
            return zero(self.get_type()[1])

    def __setitem__(self, key, newvalue):
        if self.get_type() == (SemiRing, SemiRing):
            self.__key_type = type(key)
            self.__value_type = type(newvalue)

        # if (type(key) != self.get_type()[0] or type(newvalue) != self.get_type()[1]):
        #     raise TypeError("key/value type is not match to dictionary type.")
        self.__value[key] = newvalue


class SemiRingRecord(SemiRing):

    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue is not None:
            self.__value = dict(initialvalue)
        else:
            self.__value = dict()

    def __str__(self):
        if (len(self.value.items()) == 0):
            return "<>"
        else:
            res = "< "
            for (k, v) in self.value.items():
                res += str(k) + " = " + str(v) + ", "
            res = res[:len(res)-2:] + " >"
        return res

    def __add__(self, other):
        if type(other) == SemiRingRecord and sorted(self.value.keys()) == sorted(other.value.keys()):
            pairs = {}
            if len(self.value.items()) > 0:
                for (k, v) in self.value.items():
                    pairs[k] = v + other.value[k]
                return SemiRingRecord(pairs)
            return SemiRingRecord()
        else:
            raise ValueError(str(type(self)) + " + " +
                             str(type(other)) + " is not allowed!")

    def __mul__(self, other):
        pairs = {}
        for (k, v) in self.value.items():
            pairs[k] = v * other
        return SemiRingRecord(pairs)

    def __getitem__(self, key):
        columnvalue = self.value.get(key)
        if columnvalue != None:
            return columnvalue
        else:
            return None


class SemiRingSet(SemiRingDictionary):
    def __init__(self, key_type=None, initialvalue=None):
        SemiRingDictionary.__init__(self, key_type, SemiRingBool)
        zerobool = SemiRingBool(True)
        if initialvalue != None:
            for item in initialvalue:
                self.value[item] = zerobool


class SemiRingFloat(SemiRing):

    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue == None:
            self.__value = 0
        elif not isinstance(initialvalue, float):
            raise ValueError("Value is invalid!")
        else:
            self.__value = initialvalue

    def __str__(self):
        return str(self.value)

    def __add__(self, other):
        if type(other) == SemiRingFloat:
            res = float(self.value + other.value)
            return SemiRingFloat(res)
        else:
            raise ValueError(str(type(self)) + " + " +
                             str(type(other)) + " is not allowed!")

    def __mul__(self, other):
        if type(other) == SemiRingFloat:
            res = self.value * other.value
            return SemiRingFloat(float(res))
        elif type(other) == SemiRingDictionary or type(other) == SemiRingRecord or type(other) == SemiRingSet:
            res = None
            if type(other) == SemiRingDictionary:
                res = SemiRingDictionary(
                    other.get_type()[0], other.get_type()[1])
            elif type(other) == SemiRingSet:
                res = SemiRingSet(other.get_type()[0], None)
            else:
                res = {}

            for (k, v) in other.value.items():
                res[k] = self * v

            if type(other) == SemiRingRecord:
                return SemiRingRecord(res)
            return res
        else:
            raise ValueError(str(type(self)) + " * " +
                             str(type(other)) + " is not allowed!")

    def __truediv__(self, other):
        if type(other) == SemiRingFloat:
            res = self.value / other.value
            return SemiRingFloat(float(res))
        else:
            raise ValueError(str(type(self)) + " / " +
                             str(type(other)) + " is not allowed!")

    def __sub__(self, other):
        if self != None and other != None and type(other) == type(self):
            return type(self)(self.value - other.value)
        else:
            raise ValueError("subtraction cannot be done.")


class SemiRingInt(SemiRing):

    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue == None:
            self.__value = 0
        elif not isinstance(initialvalue, int):
            raise ValueError("Value is invalid!")
        else:
            self.__value = initialvalue

    def __str__(self):
        return str(self.value)

    def __add__(self, other):
        if type(other) == SemiRingInt:
            res = int(self.value + other.value)
            return SemiRingInt(res)
        else:
            raise ValueError(str(type(self)) + " + " +
                             str(type(other)) + " is not allowed!")

    def __mul__(self, other):
        if type(other) == SemiRingInt:
            res = self.value * other.value
            return SemiRingInt(int(res))
        elif type(other) == SemiRingDictionary or type(other) == SemiRingRecord or type(other) == SemiRingSet:
            res = None
            if type(other) == SemiRingDictionary:
                res = SemiRingDictionary(
                    other.get_type()[0], other.get_type()[1])
            elif type(other) == SemiRingSet:
                res = SemiRingSet(other.get_type()[0], None)
            else:
                res = SemiRingRecord()
            for (k, v) in other.value.items():
                res[k] = self * v
            return res
        else:
            raise ValueError(str(type(self)) + " * " +
                             str(type(other)) + " is not allowed!")

    def __truediv__(self, other):
        if type(other) == SemiRingInt:
            res = self.value / other.value
            return SemiRingInt(int(res))
        else:
            raise ValueError(str(type(self)) + " / " +
                             str(type(other)) + " is not allowed!")

    def __sub__(self, other):
        if self != None and other != None and type(other) == type(self):
            return type(self)(self.value - other.value)
        else:
            raise ValueError("subtraction cannot be done.")


class SemiRingBool(SemiRing):
    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue == None:
            self.__value = False
        elif not isinstance(initialvalue, bool):
            raise ValueError("Value is invalid!")
        else:
            self.__value = initialvalue

    def __str__(self):
        return str(self.value)

    def __add__(self, other):
        if type(other) == SemiRingBool:
            res = bool(self.value + other.value)
            return SemiRingBool(res)
        else:
            raise ValueError(str(type(self)) + " + " +
                             str(type(other)) + " is not allowed!")

    def __mul__(self, other):
        if type(other) == SemiRingBool:
            res = self.value * other.value
            return SemiRingBool(bool(res))
        elif type(other) == SemiRingDictionary or type(other) == SemiRingRecord or type(other) == SemiRingSet:
            res = None
            if type(other) == SemiRingDictionary:
                res = SemiRingDictionary(
                    other.get_type()[0], other.get_type()[1])
            elif type(other) == SemiRingSet:
                res = SemiRingSet(other.get_type()[0], None)
            else:
                res = SemiRingRecord()
            for (k, v) in other.value.items():
                res[k] = self * v
            return res
        else:
            raise ValueError(str(type(self)) + " * " +
                             str(type(other)) + " is not allowed!")

    def __sub__(self, other):
        if self != None and other != None and type(other) == type(self):
            return type(self)(self.value - other.value)
        else:
            raise ValueError("subtraction cannot be done.")


class SemiRingEnum(SemiRing):
    @property
    def value(self):
        return self.__value

    def __init__(self, initialvalue=None):
        SemiRing.__init__(self)
        if initialvalue == None:
            self.__value = ""
        elif not isinstance(initialvalue, str):
            raise ValueError("Value is invalid!")
        else:
            self.__value = initialvalue

    def __str__(self):
        return str(self.value)


def dom(dict):
    if (type(dict) != SemiRingDictionary):
        raise ValueError("dom function can be done on dictionaries!")
    return sum(lambda pair: SemiRingDictionary(dict.get_type()[0], SemiRingBool, {pair[0]: SemiRingBool(True)}), dict)


def promote(scalar, to_type):
    value_type = type(scalar)
    if value_type == SemiRingInt and to_type == SemiRingFloat:
        return SemiRingFloat(float(scalar.value))
    elif value_type == SemiRingBool:
        if to_type == SemiRingInt:
            return SemiRingInt(int(scalar.value))
        elif to_type == SemiRingFloat:
            return SemiRingFloat(float(scalar.value))
    else:
        return scalar


def sum(func, dict):
    if dict == None or dict.value == {}:
        return SemiRingDictionary(SemiRing, SemiRing)
    res = None
    for pair in dict.value.items():
        tmp = func(pair)
        if res is None:
            if type(tmp) == SemiRingDictionary:
                res = SemiRingDictionary(tmp.get_type()[0], tmp.get_type()[1])
            elif type(tmp) == SemiRingSet:
                res = SemiRingSet(tmp.get_type()[0])
            else:
                res = type(tmp)()
        res = tmp + res
    if res == None:
        return dict
    return res


def selection(func, dict):
    def tmp_func(x):
        if func(x[0]):
            return SemiRingSet(SemiRingRecord, (x[0],))
        else:
            return SemiRingSet(SemiRingRecord)
    return sum(tmp_func, dict)


def projection(func, dict):
    return sum(lambda x: SemiRingSet(SemiRingRecord, (func(x[0]),)), dict)


def union(dict1, dict2):
    return dict1+dict2


def intersection(dict1, dict2):
    def tmp_func(x):
        if dict2[x[0]] == SemiRingBool(True):
            return SemiRingSet(SemiRingRecord, (x[0],))
        else:
            return SemiRingSet(SemiRingRecord)
    return sum(tmp_func, dict1)


def difference(dict1, dict2):
    def tmp_func(x):
        if dict2[x[0]] == SemiRingBool(True):
            return SemiRingSet(SemiRingRecord)
        else:
            return SemiRingSet(SemiRingRecord, (x[0],))
    return sum(tmp_func, dict1)


def recordconcat(rec1, rec2, name1, name2):
    pairs = {}
    for item in rec1.value.items():
        pairs[name1 + item[0]] = item[1]
    for item in rec2.value.items():
        pairs[name2 + item[0]] = item[1]
    return SemiRingRecord(pairs)


def cartesian_product(dict1, dict2, name1, name2):
    return sum(lambda d1: sum(lambda d2: SemiRingSet(dict1.get_type()[0], (recordconcat(d1[0], d2[0], name1, name2),)), dict2), dict1)


def join(func, dict1, dict2):
    return selection(func, cartesian_product(dict1, dict2))


def hashequaljoin(dict1, dict2, tname1, tname2, colname1, colname2):
    def partition(item):
        tmp = SemiRingDictionary(SemiRingRecord, SemiRingBool, {item})
        return SemiRingDictionary(SemiRing, SemiRingDictionary, {item[0][colname1]: tmp})

    dict1_part = sum(partition, dict1)

    def f1(dic):
        pkey = dic[0][colname2]

        def f2(dic_):
            return SemiRingSet(SemiRingRecord, ((recordconcat(dic_[0], dic[0], tname1, tname2),)))

        return sum(f2, dict1_part[pkey])

    return sum(f1, dict2)


def zero(obj):
    if (type(obj) != type):
        if (type(obj) == SemiRingBool):
            return SemiRingBool(False)
        elif (type(obj) == SemiRingInt):
            return SemiRingInt(0)
        elif (type(obj) == SemiRingFloat):
            return SemiRingFloat(0.0)
        elif (type(obj) == SemiRingSet):
            return SemiRingSet(obj.get_type()[0])
        elif (type(obj) == SemiRingDictionary):
            return SemiRingDictionary(obj.get_type()[0], obj.get_type()[1])
        elif (type(obj) == SemiRingRecord):
            pairs = {}
            if len(obj.value.items()) > 0:
                for (k, v) in obj.value.items():
                    pairs[k] = zero(v)
                return SemiRingRecord(pairs)
            return SemiRingRecord()
        else:
            return None
    else:
        if (obj == SemiRingBool):
            return SemiRingBool(False)
        elif (obj == SemiRingInt):
            return SemiRingInt(0)
        elif (obj == SemiRingFloat):
            return SemiRingFloat(0.0)
        elif (obj == SemiRingSet):
            return SemiRingSet(SemiRing)
        elif (obj == SemiRingDictionary):
            return SemiRingDictionary(SemiRing, SemiRing)
        elif (obj == SemiRingRecord):
            return SemiRingRecord()
        else:
            return None
