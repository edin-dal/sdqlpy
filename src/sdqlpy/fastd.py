class fastd:
    inner_dict = None
    fd = None

    def __init__(self, type_dict, filename):

        self.fd = __import__(filename + "_fastdict_compiled")

        if type(type_dict) != dict and len(type_dict.__class__.__name__)>8 and type_dict.__class__.__name__[0:8] == "FastDict":
            self.inner_dict = type_dict
        else:
            self.inner_dict = getattr(self.fd, "new_" + self.getAbbrDictName(type_dict))()()

    def getAbbrTypeName(self, types):
        result = ""
        if type(types)==list:
            for val in types:
                result += self.getAbbrTypeName(val) 
        else:
            if type(types) == string:
                result += "s" + str(types.max_size)
            elif types == date:
                result += "i"
            else:
                result += types.__name__[0]
        return result

    def getAbbrDictName(self, type_dict):
        return "FastDict_" + self.getAbbrTypeName(list(list(type_dict.keys())[0]._sr_dict__container.values())) + "_" + self.getAbbrTypeName(list(type_dict.values())[0])

    def from_dict(self, data_dict):
        return self.fd.from_dict(self.inner_dict, data_dict)

    def to_dict(self):
        return self.fd.to_dict(self.inner_dict)

    def set(self, key, value):
        return self.fd.set(self.inner_dict, key, value)
    
    def get(self, key):
        return self.fd.get(self.inner_dict, key)

    def print(self):
        self.fd.print(self.inner_dict)

    def size(self):
        return self.fd.size(self.inner_dict)

    def __str__(self):
        self.fd.print(self.inner_dict)
        return ""