import os
import sys
import subprocess
import csv
import functools
import inspect
import time
import statistics
import pathlib
import numpy as np
import site
from importlib import reload

class string():
    max_size = None
    def __init__(self, max_size=25):
        self.max_size = max_size

class date():
    def __init__(self):
        pass

def read_csv_to_fast_dict(file_path, header_type_dict, inclCols, delimiter):
    tmp = {}
    for col in header_type_dict.keys():
        if col in inclCols:
                tmp[col] = header_type_dict[col]
    
    outd = fastd({record(tmp): bool})
    
    header_type_dict_list = list(header_type_dict.items())
    with open(file_path, newline="\n") as csvfile:
        data = csv.reader(csvfile, delimiter=delimiter)
        output = {}
        for row in data:
            tmp = {}
            for i in range(len(row)):
                if header_type_dict_list[i][0] in inclCols:
                    if header_type_dict_list[i][1] == date:
                        tmp[header_type_dict_list[i][0]] = int("".join(row[i].split("-")))
                    elif type(header_type_dict_list[i][1]) == string:
                        tmp[header_type_dict_list[i][0]] = row[i]
                    else:
                        tmp[header_type_dict_list[i][0]] = header_type_dict_list[i][1](row[i])   
            output[record(tmp)] = True
        outd.from_dict(output)
    print("Reading " + file_path + " Finished.")
    return outd

def read_csv_to_py_dict(file_path, header_type_dict, inclCols, delimiter):
    output = {}
    header_type_dict_list = list(header_type_dict.items())
    with open(file_path, newline="\n") as csvfile:
        data = csv.reader(csvfile, delimiter=delimiter)
        for row in data:
            tmp_dict = {}
            for i in range(len(row)):
                if header_type_dict_list[i][0] in inclCols:
                    if header_type_dict_list[i][1] == date:
                        tmp_dict[header_type_dict_list[i][0]] = int("".join(row[i].split("-")))
                    elif type(header_type_dict_list[i][1]) == string:
                        tmp_dict[header_type_dict_list[i][0]] = row[i]
                    else:
                        tmp_dict[header_type_dict_list[i][0]] = header_type_dict_list[i][1](row[i])
            output[record(tmp_dict)] = True
    print("Reading " + file_path + " Finished.")
    return sr_dict(output)

def read_csv_to_np_array(file_path, header_type_dict, delimiter):
    tmp_data = []
    output_data = []
    record_type = (list(header_type_dict.keys())[0]).getContainer()
    output_headers = list(record_type.keys())
    types = list(record_type.values())

    for i in range(len(output_headers)):
        tmp_data.append([])

    with open(file_path, newline="\n") as csvfile:
        data = csv.reader(csvfile, delimiter=delimiter)
        for row in data:
            for i in range(len(row)):
                if types[i] == date:
                    tmp_data[i].append(int("".join(row[i].split("-"))))
                elif type(types[i]) == string:
                    tmp_data[i].append(row[i])
                else:
                    tmp_data[i].append(types[i](row[i]))

    for i in range(len(output_headers)):
        if type(types[i]) == string:
            targetType = "<U"+str(types[i].max_size)
            output_data.append(np.array(tmp_data[i], targetType))
        # elif types[i]== int or types[i] == date:
        #     output_data.append(np.array(tmp_data[i], "int32"))
        else:
            output_data.append(np.array(tmp_data[i]))


    # if len(inclCols)>0:
    #     includeIdx = []
    #     for header in inclCols:
    #         includeIdx.append(output_headers.index(header)) 
    #     out = []
    #     for i in range(len(output_headers)):
    #         if i in includeIdx:
    #             out.append(output_data[i])

    print("Reading " + file_path + " Finished.")

    # if len(inclCols)>0:
    #     generate_input_data_type(header_type_dict, inclCols)
    #     return sr_dict({"headers": inclCols, "data": out}, None, True)
    # else:
    return sr_dict({"headers": output_headers, "data": output_data}, None, True)

# 0: column-store | 1: pydict | 2: fast_dict
def read_csv(file_path, header_type_dict, dataset_name, delimiter='|'):
    read_mode = 0
    if read_mode == 0:
        return read_csv_to_np_array(file_path, header_type_dict, delimiter)
    # elif read_mode == 1:
    #     return read_csv_to_py_dict(file_path, header_type_dict, inclCols, delimiter)
    # elif read_mode == 2:
    #     return read_csv_to_fast_dict(file_path, header_type_dict, inclCols, delimiter)
    else:
        print("read_mode is not supported.")
        return None

#####################################################################################################################################################################

class sr_dict:
    __container = None
    __has_columnar_layout = False

    # This function is mimicking the behaviour of two seperate constructors.
    def __init__(self, initializer_dict={}, value=None, columnar_layout=False):
        if value is None:
            if initializer_dict==None:
                initializer_dict={}
            self.__container = initializer_dict
        else:
            self.__container = dict(
                [(initializer_dict, value)]
            )  # Here initializer_dict is equal to key of singleton dictionary.
        self.__has_columnar_layout = columnar_layout

    def getContainer(self):
        return self.__container

    def get(self, key):
        return self.__container.get(key)

    def __getitem__(self, key):
        return self.get(key)

    def getColumnarLayoutStatus(self):
        return self.__has_columnar_layout

    def __len__(self):
        return len(self.__container)

    def __str__(self):
        output = "{ "
        for k, v in self.__container.items():
            key = k
            val = v
            if type(k) in [str, string, np.str_]: key = "\"" + str(key) + "\""
            if type(v) in [str, string, np.str_]: val = "\"" + str(val) + "\""
            output += str(key) + ": " + str(val) + ", "
        if len(self.__container) > 0:
            output = output[0:-2]
        output += " }"
        return output

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if self.__container != None and other == None:
            return False
        elif self.__container == None and other == None:
            return True
        return self.__container == other.__container

    def __add__(self, other):
        if len(self.__container) == 0:
            return other
        elif len(other.__container) == 0:
            return self

        result = self
        for k, v in other.__container.items():
            if k in result.__container:
                result.__container[k] += v
            else:
                result.__container[k] = v
        return result

    def __truediv__(self, other):
        if type(other) == int or type(other) == float:
            return 0
        else:
            print("Error: sr_dict / number!")
            return

    def sum(self, func, is_an_update_sum=True):
        result = None
        if not self.__has_columnar_layout:
            for (k, v) in self.__container.items():
                func_result = func((k, v))
                if type(func_result) == sr_dict:
                    pass
                elif type(func_result) == dict:
                    func_result = sr_dict(func_result)
                if result is None and func_result is not None:
                    result = func_result
                elif result is not None and func_result is not None:
                    result += func_result
        else:
            for i in range(0, len(self.__container["data"][0])):
                k_initials = {}
                for j in range(len(self.__container["headers"])):
                    k_initials[self.__container["headers"][j]]=self.__container["data"][j][i]
                k = record(k_initials)
                func_result = func((k, True))
                if type(func_result) == sr_dict:
                    pass
                elif type(func_result) == dict:
                    func_result = sr_dict(func_result)
                if result is None and func_result is not None:
                    result = func_result
                elif result is not None and func_result is not None:
                    if type(func_result) == sr_dict and len(func_result)==0:
                        continue
                    result += func_result
        return result

    def joinBuild(self, col, filter, outCols):
        def outputBuilder(rec):
            output = {}
            if outCols == []:
                output[col]=rec[col]
            else:
                for c in outCols:
                    output[c]=rec[c]
            return {rec[col]: record(output)}

        return self.sum(lambda p: outputBuilder(p[0]) if filter(p) else sr_dict())

    def joinProbe(self, indexedDict, col, filter, outputFunc, is_an_update_sum=True):
        self.is_type_found = False

        def inner_lambda(p):
            v = indexedDict[p[0][col]]
            if v != None:
                return outputFunc(v, p[0])
            elif not self.is_type_found:
                self.is_type_found = True
                return sr_dict()

        res = self.sum(
            lambda p: inner_lambda(p) if filter(p) else sr_dict()
            , is_an_update_sum
        )
        return res

class record(sr_dict):
    def __init__(self, initializer_dict={}):
        sr_dict.__init__(self, initializer_dict)

    def __getattr__(self, attr):
        return self.get(attr)

    def __eq__(self, other):
        if self._sr_dict__container != None and other == None:
            return False
        elif self._sr_dict__container == None and other == None:
            return True
        vals1 = list(self._sr_dict__container.values())
        vals2 = list(other._sr_dict__container.values())
        return vals1 == vals2

    def __hash__(self):
        tmp = ""
        for k, v in self._sr_dict__container.items():
            tmp += str(v)
        return hash(tmp)

    def concat(self, other):
        res = {}
        for k,v in self._sr_dict__container.items():
            res[k] = v
        for k,v in other._sr_dict__container.items():
            res[k] = v
        return record(res)

class vector():
    __container = None
    def __init__(self, initializer_list=[]):
        self.__container = initializer_list

    def getContainer(self):
        return self.__container

    def __len__(self):
        return len(self.__container)

    def __str__(self):
        output = "[ "
        for v in self.__container:
            output += str(v) + ", "
        if len(self.__container) > 0:
            output = output[0:-2]
        output += " ]"
        return output

    def __add__(self, other):
        if len(other) == 0:
            return self
        elif len(self) == 0:
            return other
        return vector(self.__container | other.__container)

    def sum(self, func, is_an_update_sum=True):
        result = None
        for v in self.__container:
            func_result = func(v)
            if type(func_result) == sr_dict:
                pass
            elif type(func_result) == dict:
                func_result = sr_dict(func_result)
            if result is None and func_result is not None:
                result = func_result
            elif result is not None and func_result is not None:
                result += func_result
        return result

#####################################################################################################################################################################

def extractYear(full_date_in_integer_format):
    return full_date_in_integer_format//10000 

def firstIndex(string, keyword):
    return string.find(keyword)

def startsWith(string, keyword):
    return string.startswith(keyword)

def endsWith(string, keyword):
    return string.endswith(keyword)

def dictSize(dict):
    if type(dict)==sr_dict:
        return len(dict.getContainer().keys())
    elif type(dict)==vector:
        return len(dict)
    else:
        return len(dict.keys())

def substr(source, start, end):
    return source[start:end+1]

def unique(arg):
    return arg

def dense(arg1, arg2):
    return arg2

#####################################################################################################################################################################

def sdqlpy_init(execution_mode=0, threads_count=1):
    
    global run_in_python
    if execution_mode == 0:
        run_in_python = True
    elif execution_mode == 1 or execution_mode == 2:
        run_in_python = False
    else:
        print ("Execution mode is not supported. Failed.")
        return

    if execution_mode == 1:
        current_dir = str(pathlib.Path(__file__).parent.resolve())
        frame = inspect.stack()[1]
        caller_filename = frame[0].f_code.co_filename
        subprocess.call(current_dir + "/compiler.sh " + current_dir + " " + caller_filename + " " + str(threads_count), shell=True)

def sdql_compile(in_type):  
    reload(site)
    def actual_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if run_in_python:
                if args[0].__class__.__name__ == "fastd":
                    print("fast_dict mode cannot be run in Python mode.")
                    return
                else:
                    return func(*args, **kwargs)
            try:
                mod_name = inspect.getfile(func).split("/")[-1].split(".")[0]+"_compiled"
                mod = __import__(mod_name)
                # mod = __import__("sdqlpy_compiled_"+inspect.getfile(func).split("/")[-1].split(".")[0] + "." + mod_name)
                # mod = importlib.import_module("sdqlpy_compiled_"+inspect.getfile(func).split("/")[-1].split(".")[0] + "." + mod_name)
                
            except ImportError as e:
                print("Error: the compiled library related to the current code is not found!")
                print(e)
                return
            fname = func.__name__ + "_compiled"

            if hasattr(mod, fname):
                db = None
                if args[0].__class__.__name__ == "fastd":
                    db = {}
                    argNames = inspect.getfullargspec(func).args
                    for i in range(len(args)):
                        db[argNames[i]] = args[i].inner_dict
                    return getattr(mod, fname)(db) 
                elif args[0].getColumnarLayoutStatus():
                    db = []
                    for arg in args:
                        db.append(arg["data"])
                    return getattr(mod, fname)(db)
                else:
                    db = {}
                    argNames = inspect.getfullargspec(func).args
                    for i in range(len(args)):
                        db[argNames[i]] = args[i]
                    return getattr(mod, fname)(db)                    
            else:
                print("Error: the compiled version of " + func.__name__ + " is not found!")
                return None
        return wrapper
    return actual_decorator

def benchmark(title, iterations, func, args, show_results=True, verbose=True):
    if str(os.getenv('NO_HYPER_THREADING')) != "1":
        os.system('echo on > /sys/devices/system/cpu/smt/control')
    else:
        if sys.platform != "darwin":
            os.system('echo off > /sys/devices/system/cpu/smt/control')
    totalTime=0
    timesList = []
    func(*args)
    for i in range(0, iterations):
        start=(time.time() * 1000)
        func(*args)
        end=(time.time() * 1000)
        totalTime+=end-start
        timesList.append(end-start)
    res = func(*args)
    if verbose:
        print(title + ": Mean: " + "{0:0.2f}".format(((totalTime*1.0)/iterations)) + " | StDev: " + "{0:0.2f}".format(statistics.stdev(timesList)))
        if show_results:
            if (res.__class__.__name__) == "fastd":
                print(res)
                print("Result Size: " + str(res.size()))
            else:
                print(str(res))
                try:
                    print("Result Size:" + str(len(res)))
                except:
                    print("Scalar Result")
        else:
            try:
                if (res.__class__.__name__) == "fastd":
                    print("Result Size: " + str(res.size()))
                else:
                    print("Result Size:" + str(len(res)))
            except:
                print("Scalar Result")
        print("============================================================================")
    else:
        print(title + "\t" + "{0:0.2f}".format(((totalTime*1.0)/iterations)))