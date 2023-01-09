from fileinput import filename
import os
import sys
import ast
from sdql_ir import *
    
class searchVisitor(ast.NodeVisitor):
    condition = None
    results = []

    def findNode(self, rootNode, conditionFunc):
        self.condition = conditionFunc
        self.visit(rootNode)
        return self.results

    def generic_visit(self, node):
        if self.condition(node):
            self.results.append(node)
            return
        else:
            ast.NodeVisitor.generic_visit(self, node)

def getAbbrTypeName(targettype):
    result = ""
    
    if type(targettype) == RecordType:
        targettype = targettype.typesList
        for p in targettype:
            if type(p[1])==StringType:
                result += "s" + str(p[1].charCount)
            elif type(p[1])==IntType:
                result += "i"
            elif type(p[1])==FloatType:
                result += "f"
            elif type(p[1])==BoolType:
                result += "b"
            else:
                print("Error: unknown value type!")
                return
    else:
        if type(targettype)==StringType:
            result += "s" + str(targettype.charCount)
        elif type(targettype)==IntType:
            result += "i"
        elif type(targettype)==FloatType:
            result += "f"
        elif type(targettype)==BoolType:
            result += "b"
        else:
            print("Error: unknown value type!")
            return

    return result

def getAbbrDictName(keyType, valType):
    return "FastDict_" + getAbbrTypeName(keyType) + "_" + getAbbrTypeName(valType)

def getCPPType(targettype):

    result = ""
    
    if type(targettype) == RecordType:
        targettype = targettype.typesList
        result += "tuple<"
        for p in targettype:
            if type(p[1])==StringType:
                result += "VarChar<" + str(p[1].charCount) + ">, "
            elif type(p[1])==IntType:
                result += "long, "
            elif type(p[1])==FloatType:
                result += "double, "
            elif type(p[1])==BoolType:
                result += "bool, "
            else:
                print("Error: unknown value type!")
                return
        result = result[:-2]
        result += ">"
    else:
        p = targettype
        if type(p)==StringType:
            result += "VarChar<" + str(p[1].charCount) + ">, "
        elif type(p)==IntType:
            result += "long, "
        elif type(p)==FloatType:
            result += "double, "
        elif type(p)==BoolType:
            result += "bool, "
        else:
            print("Error: unknown value type!")
            return
        result = result[:-2]
    return result

def getCPPDictType(keyType, valType):
    return "phmap::flat_hash_map<" + getCPPType(keyType) + ", " + getCPPType(valType) + ">"

def getAllDictDefinitions(output_dicts):
    result = ""
    
    for v in output_dicts.values():
        abbr = getAbbrDictName(v.fromType, BoolType())
        cpp = getCPPDictType(v.fromType, BoolType())

        result += """
////////////////////////////////////
"""

        result += """
typedef struct {
    PyObject_HEAD
    """ + cpp + """* dict;
} """ + abbr + """;
"""
    result += """
////////////////////////////////////

"""
    return result

def generate_fastdicts(output_dicts, file_path, lib_dir):

    dir_path = os.path.dirname(os.path.abspath(file_path))

    setupCode = """    
from setuptools import setup, find_packages, Extension
import sysconfig
import numpy as np
import os
from sys import argv

extra_compile_args = sysconfig.get_config_var('CFLAGS').split()
extra_compile_args += ["--std=c++17", "-O3", "-ltbb", "-Wno-unused-variable", "-D_LIBCPP_DISABLE_AVAILABILITY"]

module1 = Extension(
    '""" + file_path[:-3] + "_fastdict_compiled" + """',
    sources=['""" + dir_path + """/fast_dict.cpp'],
    include_dirs=[],
    extra_compile_args=extra_compile_args,
    extra_link_args=["-ltbb", "-Wno-unused-variable"]
    )

module2 = Extension(
    '""" + file_path[:-3] + "_compiled" + """',
    sources=['""" + dir_path + """/""" + file_path[:-3] + """_compiled.cpp'],
    include_dirs=[np.get_include()],
    extra_compile_args=extra_compile_args,
    extra_link_args=["-ltbb", "-Wno-unused-variable"]
    )
    
setup(
    name=\'sdqlpy_""" + "compiled_" + file_path[:-3] + """\',
    version='1.0',
    packages=find_packages(),
    url='',
    license='',
    author='Hesam Shahrokhi',
    author_email='',
    description='',
    ext_modules=[module1, module2],
)

os.remove('""" + dir_path + """/fast_dict.cpp')
os.remove('""" + dir_path + """/""" + file_path[:-3] + """_compiled.cpp')
os.remove(argv[0])

"""

    with open(dir_path + "/fast_dict_setup.py", "w") as myfile:
        myfile.write(setupCode)
        myfile.close()

    print(">>> fast_dict setup.py file generated.")


    cCode = ""

    cCode += """
#include <Python.h>
#include \"""" + lib_dir + """include/headers.h"

using namespace std;

/*
static string GetType(PyObject *obj)
{
    PyTypeObject* type = obj->ob_type;
    const char* p = type->tp_name;
    return string(p);
}
*/

"""
    
    for v in output_dicts.values():
        abbr = getAbbrDictName(v.fromType, BoolType())
        cpp = getCPPDictType(v.fromType, BoolType())
        rec_keys = list(list(zip(*v.fromType.typesList))[0])
        rec_vals = list(list(zip(*v.fromType.typesList))[1])
        cCode += """
////////////////////////////////////////////////////////////////////

typedef struct {
    PyObject_HEAD
    """ + cpp + """* dict;
    vector<string> cols;
} """ + abbr + """;


static PyTypeObject """ + abbr + """_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    \"fast_dict.""" + abbr + """\"
    };

static PyMethodDef """ + abbr + """_methods[] = {
    {NULL, NULL}		/* sentinel */
};

static int """ + abbr + """_init(""" + abbr + """ *self, PyObject *args, PyObject *kwds)
// initialize """ + abbr + """ Object
{
    self->dict = new """ + cpp + """();
    """

        for col in rec_keys:
            cCode += "self->cols.push_back(\"" + col + "\");\n\t"

        cCode += """
    return 0;
}

static void """ + abbr + """_Type_init()
{
    """ + abbr + """_Type.tp_new = PyType_GenericNew;
    """ + abbr + """_Type.tp_basicsize=sizeof(""" + abbr + """);
    """ + abbr + """_Type.tp_flags=Py_TPFLAGS_DEFAULT;
    """ + abbr + """_Type.tp_methods=""" + abbr + """_methods;
    """ + abbr + """_Type.tp_init=(initproc)""" + abbr + """_init;
}

static PyObject * """ + abbr + """_Set(""" + abbr + """ *self, PyObject *pykey, PyObject *pyval)
{ 
    PyObject* pykey_data = PyDict_Values(PyObject_GetAttrString(pykey, "_sr_dict__container"));

    const auto& key = make_tuple("""
    
        counter=0 
        for ty in rec_vals:
            if type(ty)==IntType:
                cCode += "PyLong_AsLong(PyList_GetItem(pykey_data, " + str(counter) + ")), "
            elif type(ty)==FloatType:
                cCode += "PyFloat_AsDouble(PyList_GetItem(pykey_data, " + str(counter) + ")), "
            elif type(ty)==StringType:
                cCode += "VarChar<" + str(ty.charCount) + ">(PyUnicode_AsWideCharString(PyList_GetItem(pykey_data, " + str(counter) + "), NULL)), "
            else:
                print("Error: type is not supported!!: " + str(ty))
            counter += 1

        cCode = cCode[:-2] + ");"

        cCode += """
    (*(self->dict))[key] = true;

    Py_RETURN_TRUE;
}


static PyObject * """ + abbr + """_Get(""" + abbr + """ *self, PyObject *pykey)
{ 
    PyObject* pykey_data = PyDict_Values(PyObject_GetAttrString(pykey, "_sr_dict__container"));

    const auto& key = make_tuple("""
    
        counter=0 
        for ty in rec_vals:
            if type(ty)==IntType:
                cCode += "PyLong_AsLong(PyList_GetItem(pykey_data, " + str(counter) + ")), "
            elif type(ty)==FloatType:
                cCode += "PyFloat_AsDouble(PyList_GetItem(pykey_data, " + str(counter) + ")), "
            elif type(ty)==StringType:
                cCode += "VarChar<" + str(ty.charCount) + ">(PyUnicode_AsWideCharString(PyList_GetItem(pykey_data, " + str(counter) + "), NULL)), "
            else:
                print("Error: type is not supported!!: " + str(ty))
            counter += 1

        cCode = cCode[:-2] + ");"

        cCode += """
    if ((*(self->dict))[key] == true)
        Py_RETURN_TRUE;
    else
        Py_RETURN_FALSE;
}



static PyObject * """ + abbr + """_FromDict(""" + abbr + """ *self, PyObject *data_dict)
{ 

    PyObject *key, *value;
    Py_ssize_t pos = 0;

    while(auto pair = PyDict_Next(data_dict, &pos, &key, &value))
    {
        """ + abbr + """_Set(self, key, value);
    }


    Py_RETURN_TRUE;
}


static PyObject * """ + abbr + """_ToDict(""" + abbr + """ *self)
{ 
    PyObject* recType = PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdql_lib")), "record");
    PyObject* srdictType = PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdql_lib")), "sr_dict");

    PyObject* outdict = PyDict_New();

    for (auto& p : *(self->dict))
    {
        PyObject* tmprecdict = PyDict_New();
        """   
        counter=0 
        for ty in rec_vals:
            cCode += """PyDict_SetItemString(tmprecdict, ((self->cols)[""" + str(counter) + """]).c_str(), """
            if type(ty)==IntType:
                cCode += "PyLong_FromLong(get<" + str(counter) + ">(p.first)));\n\t\t"
            elif type(ty)==FloatType:
                cCode += "PyFloat_FromDouble(get<" + str(counter) + ">(p.first)));\n\t\t"
            elif type(ty)==StringType:
                cCode += "PyUnicode_FromWideChar((Py_UNICODE*)(get<" + str(counter) + ">(p.first)).data, " + str(ty.charCount) + "));\n\t\t"
            else:
                print("Error: type is not supported!: " + str(ty))
            counter += 1
        cCode += """

        PyDict_SetItem(outdict, PyObject_CallObject(recType, Py_BuildValue("(O)", tmprecdict)), Py_True);
    }

    return (PyObject*)PyObject_CallObject(srdictType, Py_BuildValue("(O)", outdict));
}


static PyObject * """ + abbr + """_Print(""" + abbr + """ *self)
{
    cout << *(self->dict) << endl;
    Py_RETURN_TRUE;
}

static PyObject * """ + abbr + """_Size(""" + abbr + """ *self)
{
    return PyLong_FromLong((*(self->dict)).size());
}

"""

    cCode += """

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

"""

    for dict in output_dicts.values():
        abbr = getAbbrDictName(dict.fromType, BoolType())
        cCode += """

static PyObject* New_""" + abbr + """(PyObject *self, PyObject *args)
{
    return (PyObject*)(&""" + abbr + """_Type);
}

"""
    cCode += """
static PyObject* Set(PyObject *self, PyObject *args)
{
    PyObject * dict;
    PyObject * pykey;
    PyObject * pyval;

    if (!PyArg_ParseTuple(args, "OOO", &dict, &pykey, &pyval)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Set");
        return Py_False;
    }
"""
    counter = 0
    for dict in output_dicts.values():
        abbr = getAbbrDictName(dict.fromType, BoolType())
        
        if counter == 0:
            cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Set(("""+ abbr + """*)dict, pykey, pyval);
        """
        else:
            cCode += """
    else if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Set(("""+ abbr + """*)dict, pykey, pyval);
"""
        counter += 1

    cCode += """
    else
    {
        cout << "Set: Type is not defined!" << endl;
        return Py_False;
    }
}
"""
 
    cCode += """
static PyObject* Get(PyObject *self, PyObject *args)
{
    PyObject * dict;
    PyObject * pykey;

    if (!PyArg_ParseTuple(args, "OO", &dict, &pykey)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Set");
        return Py_False;
    }
"""
    counter = 0
    for dict in output_dicts.values():
        abbr = getAbbrDictName(dict.fromType, BoolType())
        
        if counter == 0:
            cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Get(("""+ abbr + """*)dict, pykey);
        """
        else:
            cCode += """
    else if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Get(("""+ abbr + """*)dict, pykey);
"""
        counter += 1

    cCode += """
    else
    {
        cout << "Set: Type is not defined!" << endl;
        return Py_False;
    }
}
"""

 
    cCode += """
static PyObject* FromDict(PyObject *self, PyObject *args)
{
    PyObject * dict;
    PyObject * data_dict;

    if (!PyArg_ParseTuple(args, "OO", &dict, &data_dict)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Set");
        return Py_False;
    }
"""
    counter = 0
    for dict in output_dicts.values():
        abbr = getAbbrDictName(dict.fromType, BoolType())
        
        if counter == 0:
            cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_FromDict(("""+ abbr + """*)dict, data_dict);
        """
        else:
            cCode += """
    else if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_FromDict(("""+ abbr + """*)dict, data_dict);
"""
        counter += 1

    cCode += """
    else
    {
        cout << "Set: Type is not defined!" << endl;
        return Py_False;
    }
}
"""

 
    cCode += """
static PyObject* ToDict(PyObject *self, PyObject *args)
{
    PyObject * dict;

    if (!PyArg_ParseTuple(args, "O", &dict)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Set");
        return Py_False;
    }
"""
    counter = 0
    for dict in output_dicts.values():
        abbr = getAbbrDictName(dict.fromType, BoolType())
        
        if counter == 0:
            cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_ToDict(("""+ abbr + """*)dict);
        """
        else:
            cCode += """
    else if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_ToDict(("""+ abbr + """*)dict);
"""
        counter += 1

    cCode += """
    else
    {
        cout << "Set: Type is not defined!" << endl;
        return Py_False;
    }
}
"""


    cCode += """
static PyObject* Size(PyObject *self, PyObject *args)
{
    PyObject * dict;

    if (!PyArg_ParseTuple(args, "O", &dict)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Size");
        return Py_False;
    }
"""

    counter = 0
    for dict in output_dicts.values():
        abbr = getAbbrDictName(dict.fromType, BoolType())

        if counter == 0:
            cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Size(("""+ abbr + """*)dict);
        """
        else:
            cCode += """
    else if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Size(("""+ abbr + """*)dict);
"""
        counter += 1
    cCode += """
    else
    {
        cout << "Size: Type is not defined!" << endl;
        return Py_False;
    }
}



static PyObject* Print(PyObject *self, PyObject *args)
{
    PyObject * dict;

    if (!PyArg_ParseTuple(args, "O", &dict)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing: Size");
        return Py_False;
    }
"""

    counter = 0
    for dict in output_dicts.values():
        abbr = getAbbrDictName(dict.fromType, BoolType())
        if counter == 0:
            cCode += """
    if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Print(("""+ abbr + """*)dict);
        """
        else:
            cCode += """
    else if (PyObject_IsInstance(dict, (PyObject*)(&"""+ abbr + """_Type)))
        return """+ abbr + """_Print(("""+ abbr + """*)dict);
"""
        counter += 1
    cCode += """
    else
    {
        cout << "Print: Type is not defined!" << endl;
        return Py_False;
    }
}

static PyMethodDef fast_dict_methods[] = {
{"set", Set, METH_VARARGS, ""},
{"get", Get, METH_VARARGS, ""},
{"from_dict", FromDict, METH_VARARGS, ""},
{"to_dict", ToDict, METH_VARARGS, ""},
{"size", Size, METH_VARARGS, ""},
{"print", Print, METH_VARARGS, ""},"""
    for dict in output_dicts.values():
        abbr = getAbbrDictName(dict.fromType, BoolType())
        cCode += """
{"new_""" + abbr + """\", New_""" + abbr + """, METH_VARARGS, ""},"""
    cCode += """{NULL,		NULL}		/* sentinel */
};

static struct PyModuleDef fast_dict = 
{
    PyModuleDef_HEAD_INIT,
    \"fast_dict\",
    "Documentation",
    -1,
    fast_dict_methods, NULL, NULL, NULL, NULL
};

PyMODINIT_FUNC PyInit_""" + file_path[:-3] + """_fastdict_compiled(void) 
{
    """
    for dict in output_dicts.values():
        abbr = getAbbrDictName(dict.fromType, BoolType())
        cCode += """
    """ + abbr + """_Type_init();  
    if (PyType_Ready(&""" + abbr + """_Type) < 0)
        cout << "Type Not Ready!" << endl;
    """
    cCode += """

    return PyModule_Create(&fast_dict);

}
"""
    
    with open(dir_path + "/fast_dict.cpp", "w") as myfile:
        myfile.write(cCode)
        myfile.close()

    print(">>> fast_dict.py file generated.")