import sys
import ast
import os
from importlib.machinery import SourceFileLoader

from sdql_ir import *
from fast_dict_generator import *

################################################################################

def mapping(datasetName):
    return "db->" + datasetName + "_dataset"

class visitor(ast.NodeVisitor):
    code = ""
    func_name = None
    newVarCounter = 1
    isInJoinFilterLambda = False
    finalClosingPranthesis = ""
    varNames = []
    is_columnar_input = None
    is_assignment_sum = False
    dense_size = None

    def __init__(self, f_name, is_columnar_input):
        self.func_name = f_name
        self.is_columnar_input = is_columnar_input

    def visit_FunctionDef(self, node):
 
        for arg in node.args.args:
            if self.is_columnar_input:
                self.code += arg.arg + " = VarExpr(\"" + mapping(arg.arg) + "\")\n"
            else:
                self.code += arg.arg + " = VarExpr(\"" + arg.arg + "\")\n"

        self.code += "\n\n" + node.name + "="

        for b in node.body:
            self.visit(b)

    def visit_Call(self, node):
        if type(node.func)==ast.Attribute and node.func.attr=="sum":
            self.code += "SumBuilder("
            self.visit_Lambda(node.args[0])
            if type(node.func.value)==ast.Name:
                self.code += ", " + node.func.value.id + ", "
            elif (type(node.func.value)==ast.Call and type(node.func.value.func)==ast.Attribute):
                self.code += ", "
                self.visit_Call(node.func.value)
                self.code += ", "
            elif (type(node.func.value)==ast.Subscript):
                self.code += ", "
                self.visit_Subscript(node.func.value)
                self.code += ", "   
            elif (type(node.func.value)==ast.Attribute):
                self.code += ", "
                self.visit_Attribute(node.func.value)
                self.code += ", "   
            else:
                print ("Error: Unknown Sum Dict Type! Type: " + str(node.func.value))

            # if len(node.args)>1:
            if self.is_assignment_sum:
                self.code += "True"
                self.is_assignment_sum = False
            else:
                self.code += "False"
                # self.code += str(not node.args[1].value)

            if len(node.args)>1:
                if not (node.args[1].value):
                    self.code += ", \"phmap::flat_hash_map\""
            else:
                if self.dense_size != None:
                    self.code += ", \"dense_array(" + str(self.dense_size) + ")\""
                    self.dense_size = None
                
                # else:
                #     self.code += ", \"" + node.args[1].value + "\""
            
            self.code += ")"
            
        elif type(node.func)==ast.Name and node.func.id=="extractYear":
            self.code += "ExtFuncExpr(ExtFuncSymbol.ExtractYear, "
            self.visit(node.args[0])
            self.code += ", ConstantExpr(\"Nothing!\"), ConstantExpr(\"Nothing!\"))"

        elif type(node.func)==ast.Name and node.func.id=="startsWith":
            self.code += "ExtFuncExpr(ExtFuncSymbol.StartsWith, "
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.args[1])
            self.code += ", ConstantExpr(\"Nothing!\"))"

        elif type(node.func)==ast.Name and node.func.id=="endsWith":
            self.code += "ExtFuncExpr(ExtFuncSymbol.EndsWith, "
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.args[1])
            self.code += ", ConstantExpr(\"Nothing!\"))"
        
        elif type(node.func)==ast.Name and node.func.id=="dictSize":
            self.code += "ExtFuncExpr(ExtFuncSymbol.DictSize, "
            self.visit(node.args[0])
            self.code += ", ConstantExpr(\"Nothing!\"), ConstantExpr(\"Nothing!\"))"

        elif type(node.func)==ast.Name and node.func.id=="substr":
            self.code += "ExtFuncExpr(ExtFuncSymbol.SubStr, "
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.args[1])
            self.code += ", "
            self.visit(node.args[2])
            self.code += ")"

        elif type(node.func)==ast.Name and node.func.id=="firstIndex":
            self.code += "ExtFuncExpr(ExtFuncSymbol.FirstIndex, "
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.args[1])
            self.code += ", ConstantExpr(\"Nothing!\"))"

        elif type(node.func)==ast.Name and node.func.id=="sr_dict":
            self.visit_Dict(node.args[0])

        elif type(node.func)==ast.Name and node.func.id=="record":
            self.code += "RecConsExpr(["
            
            counter = 0
            for key in node.args[0].keys:
                self.code += "(\""
                self.code += key.value
                self.code += "\", "
                self.visit(node.args[0].values[counter])
                self.code += "), "
                counter += 1
            self.code = self.code[:-2]

            self.code += "])"

        elif type(node.func)==ast.Name and node.func.id=="vector":
            self.code += "VecConsExpr([" 
            self.visit(node.args[0])
            self.code += "])"

        elif type(node.func)==ast.Attribute and node.func.attr=="concat":
            self.code += "ConcatExpr("
            self.visit(node.func.value)
            self.code += ", "
            self.visit(node.args[0])
            self.code += ")"

        elif type(node.func)==ast.Name and node.func.id=="unique":
            self.is_assignment_sum = True
            self.visit(node.args[0])

        elif type(node.func)==ast.Name and node.func.id=="dense":
            self.dense_size = node.args[0].value
            self.visit(node.args[1])

        elif type(node.func)==ast.Attribute and node.func.attr=="joinBuild":
            self.code += "JoinPartitionBuilder("
            self.visit(node.func.value)
            self.code += ", \""
            self.code += node.args[0].value
            self.code += "\", "
            self.isInJoinFilterLambda = True
            self.visit(node.args[1])
            self.isInJoinFilterLambda = False
            self.code += ", ["
            
            if len(node.args[2].elts)>0:
                for el in node.args[2].elts:
                    self.code += "\"" + el.value + "\", "
                self.code = self.code[:-2] + "]"
            else:
                self.code += "]"

            if len(node.args)>3:
                if not (node.args[3].value):
                    self.code += ", \"phmap::flat_hash_map\""
                # else:
                #     self.code += ", \"" + node.args[3].value + "\""


            self.code += ")"

        elif type(node.func)==ast.Attribute and node.func.attr=="joinProbe":
            self.code += "JoinProbeBuilder("
            self.visit(node.args[0])
            self.code += ", "
            self.visit(node.func.value)
            self.code += ", \""
            self.code += node.args[1].value
            self.code += "\", "
            self.isInJoinFilterLambda = True
            self.visit(node.args[2])
            self.isInJoinFilterLambda = False
            self.code += ", "
            self.visit(node.args[3])

            if len(node.args)>4:
                self.code += ", "
                self.code += str(not node.args[4].value)

            if len(node.args)>5:
                self.code += ", \"" + ("phmap::parallel_flat_hash_map" if node.args[5].value else "phmap::flat_hash_map") + "\""

            self.code += ")"
        elif type(node.func)==ast.Name and node.func.id=="sdql_compile":
            pass
        else:
            print ("Error: Unknown Call Node!: " + str(node.func.id))
    
    def visit_Lambda(self, node):
        self.code += "lambda "
        for arg in node.args.args:
            self.code += arg.arg + ", "
        self.code = self.code[:-2] + ": "
        self.visit(node.body)
    
    def visit_IfExp(self, node):
        self.code += "IfExpr("
        self.visit(node.test)
        self.code += ", "
        self.visit(node.body)
        self.code += ", "
        # self.code += ", ConstantExpr(0)"
        self.visit(node.orelse)
        self.code += ")"

    def visit_Compare(self, node):
        if type(node.ops[0])==ast.In:
            self.code += "ExtFuncExpr(ExtFuncSymbol.StringContains, " 
            self.visit(node.left)
            self.code += ", ConstantExpr(-1), "
            self.visit(node.comparators[0])
            self.code += ") == ConstantExpr(True)"
            return
        self.code += "("
        self.visit(node.left)
        if type(node.ops[0])==ast.LtE:
            self.code += " <= "
        elif type(node.ops[0])==ast.GtE:
            self.code += " >= "
        elif type(node.ops[0])==ast.Lt:
            self.code += " < "
        elif type(node.ops[0])==ast.Gt:
            self.code += " > "
        elif type(node.ops[0])==ast.Eq:
            self.code += " == "
        elif type(node.ops[0])==ast.NotEq:

            self.code += " != "
        else:
            print ("Error: Unknown Compare Node! | " + str(type(node.ops[0])))
        self.visit(node.comparators[0])
        self.code += ")"

    def visit_Constant(self, node):
        if type(node.value) == str:
            self.code += "ConstantExpr(\"" + node.value + "\")"
        else:
            self.code += "ConstantExpr(" + str(node.value) + ")"

    def visit_UnaryOp(self, node):
        if type(node.op) == ast.USub:
            self.code += "ConstantExpr(-1)"
        else:
            print("Unary not defined!")
            return
        self.code += "*("
        self.visit(node.operand)
        self.code += ")" 

    def visit_BoolOp(self, node):
        op = None
        if type(node.op)==ast.And:
            op = " * "
        elif type(node.op)==ast.Or:
            op = " + "
        else:
            print ("Error: Unknown BoolOp Node!")
        self.code += "("
        for val in node.values:
            self.code += "("
            self.visit(val)
            self.code += ")"
            self.code += op
        self.code = self.code[:-3]
        self.code += ")"

    def visit_BinOp(self, node):
        self.code += "("
        self.visit(node.left)
        if type(node.op)==ast.Mult:
            self.code += " * "
        elif type(node.op)==ast.Sub:
            self.code += " - "
        elif type(node.op)==ast.Add:
            self.code += " + "
        elif type(node.op)==ast.Div:
            self.code += " / "
        else:
            print ("Error: Unknown BinOp Node!")
        self.visit(node.right)
        self.code += ")"

    def visit_Name(self, node):
        self.code += node.id

    def visit_Subscript(self, node):
        if self.isInJoinFilterLambda:
            if type(node.value) == ast.Subscript:
                self.code += "("
                self.visit_Subscript(node.value)
                self.code += ")["
                self.visit(node.slice.value)
                self.code += "]"
            elif type(node.slice.value) in [ast.Call, ast.Attribute]:
                self.code += node.value.id + "["
                self.visit(node.slice.value)
                self.code += "]"
            else:
                self.code += node.value.id
            return
        elif type(node.slice.value) == ast.Constant:
            self.code += node.value.id + "[" + str(node.slice.value.value) + "]"
        else:
            self.code += node.value.id + "["
            self.visit(node.slice.value)
            self.code += "]"


    def visit_Attribute(self, node):
        self.visit(node.value)
        self.code += "." + node.attr

    def visit_Return(self, node):
        self.code += "LetExpr(VarExpr(\"out\"), "
        self.visit(node.value)
        self.code += ", ConstantExpr(True))"

    def visit_Expr(self, node):
        self.visit(node.value)

    def visit_Dict(self, node):
        self.code += "DicConsExpr([("
        self.visit(node.keys[0])
        self.code += ", "
        self.visit(node.values[0])
        self.code += ")])"

    def visit_Assign(self, node):
        self.varNames.append(node.targets[0].id)
        self.code += "LetExpr(" + node.targets[0].id + ", "
        self.visit(node.value)
        self.code += ", "
        self.finalClosingPranthesis += ")"

    def visit_Index(self, node):
        self.visit(node.value)

    def visit(self, node):
        ast.NodeVisitor.visit(self, node)

    def generic_visit(self, node):
        print (">>> Generic Visit: " + type(node).__name__)
        ast.NodeVisitor.generic_visit(self, node)


class searchVisitor(ast.NodeVisitor):
    condition = None
    foundNode = None

    def findNode(self, rootNode, conditionFunc):
        self.condition = conditionFunc
        self.visit(rootNode)
        return self.foundNode

    def generic_visit(self, node):
        if self.condition(node):
            self.foundNode = node
            return
        else:
            ast.NodeVisitor.generic_visit(self, node)

class searchVisitor2(ast.NodeVisitor):
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

def GenerateIRFile(fileName, code, path=""):
    with open(os.path.join(os.path.dirname(__file__), path) + fileName + ".py", "w") as myfile:
        myfile.write(code)
        myfile.close()
    # print(">>> " + str(fileName) + ": IR code generated.")

def RemoveFile(path):
    os.remove(path)

def CreateCPPFile(code, name, path):
    with open(os.path.join(os.path.dirname(__file__), path) + name + "_compiled.cpp", "w") as myfile:
        myfile.write(code)
        myfile.close()
    print(">>> " + str(name) + ": C++ code generated.")

def CreateSetupFile(code, name, path):
    with open(os.path.join(os.path.dirname(__file__), path) + name + "_compiled_setup.py", "w") as myfile:
        myfile.write(code)
        myfile.close()
    print(">>> setup.py file generated.")

################################################################################

def main():
    file_name = ""
    is_columnar_input = True
    is_fast_dict_input = False
    cores = 1
    file_dir = ""
    all_functions_in_types = {}
    all_functions_out_types = {}
    cppcode_first = ""

    if len(sys.argv) == 5:
        file_name = sys.argv[1]
        is_columnar_input = False if sys.argv[2]=="0" else True
        cores = sys.argv[3]
        file_dir = sys.argv[4]
        print ('>>> Processing File:', file_name)
    else:
        print ('>>> Compiler command parameters are not correct!')
    
    with open(file_name) as file:
        node = ast.parse(file.read())

    functions = [
        n for n in node.body 
        if isinstance(n, ast.FunctionDef) and len(n.decorator_list)>0 and n.decorator_list[0].func.id=="sdql_compile"
        ]

    cppcode = """
#include <Python.h>
#include "numpy/arrayobject.h"
#include \"""" + file_dir + """include/headers.h"
"""

    cppcode_first = cppcode
    cppcode = ""

    cppcode += """
int init_numpy(){import_array();return 0;}

/*
static string GetType(PyObject *obj)
{
    PyTypeObject* type = obj->ob_type;
    const char* p = type->tp_name;
    return string(p);
}
*/

static wchar_t* ConstantString(const char* data, int len)
{
    wchar_t* wc = new wchar_t[len]; 
    mbstowcs (wc, data, len); 
    return wc;
} 

using namespace std;

tbb::task_scheduler_init scheduler(""" + cores + """);

"""

    if is_columnar_input:
        cppcode += """

class DB
{
    public:
"""
    
    searchViss = searchVisitor2()
    read_csv_nodes = searchViss.findNode(node, 
        lambda n: type(n) == ast.Assign and type(n.value) == ast.Call and type(n.value.func) == ast.Name and n.value.func.id == "read_csv"
    )

    for r in read_csv_nodes:
        dataset_name = r.value.args[2].value
        cppcode += "int " + dataset_name + "_dataset_size = 0;\n"

    cppcode += """
};

"""
    
    for function in functions:
        f_name = function.name
        print(">>>>>> Processing Function:", f_name)
        f_decorators = function.decorator_list
        sdql_decorator_node = f_decorators[0]
        sdql_decorator_node_in_type = sdql_decorator_node.args[0]
           
        searchVis = searchVisitor()

        vis = visitor(f_name, is_columnar_input)
        vis.varNames = []
        vis.visit(ast.parse(function))
        vis.code += vis.finalClosingPranthesis
        vis.code += "\n\n\n" + f_name + "_typecache = {}\n"

    
        topCode = ""
        topCode += "import os\nimport sys\n"
        # topCode += "sys.path.append(\"" + file_dir + "lib/\")\n"
        topCode += "from sdql import *\n"
        topCode += "from sdql_ir import *\n"
        topCode += "from sdql_ir_type_inference import *\n"
        topCode += "import sdql_ir_cpp_generator_par as gen\n\n\n"

        varDeclaration = ""
        for var in vis.varNames:
            varDeclaration += var + " = VarExpr(\"" + var + "\")\n"
        
        vis.code = topCode + varDeclaration + "\n\n" + vis.code

        counter = 0
        input_types_dict = "{"

        for type_var_name in sdql_decorator_node_in_type.values:

            in_type_node = None
            in_type_node = searchVis.findNode(node, lambda n:
                    type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == type_var_name.id).value

            if is_columnar_input:
                input_types_dict += "\"" + mapping(sdql_decorator_node_in_type.keys[counter].value) + "\": "
            else:
                input_types_dict += "\"" + sdql_decorator_node_in_type.keys[counter].value + "\": "
            if type(in_type_node) == ast.Dict:
                input_types_dict += "DictionaryType(RecordType(["
                recordKeys = in_type_node.keys[0].args[0].keys
                recordVals = in_type_node.keys[0].args[0].values
                for i in range(0 , len(recordKeys)):
                    input_types_dict += "(\"" + recordKeys[i].value + "\", "
                    if type(recordVals[i]) == ast.Call and recordVals[i].func.id == "string":
                        input_types_dict += "StringType(" + str(recordVals[i].args[0].value if len(recordVals[i].args) else 25) + ")"
                    elif recordVals[i].id == "date":
                        input_types_dict += "IntType()"
                    elif recordVals[i].id == "int":
                        input_types_dict += "IntType()"
                    elif recordVals[i].id == "float":
                        input_types_dict += "FloatType()"
                    input_types_dict += "), " 
                input_types_dict = input_types_dict[:-2]
                input_types_dict += "]), BoolType()), "
            else:
                print("Error: in_type is unknown!")
                return

           
            counter += 1

        input_types_dict = input_types_dict[:-2] + "}"
        vis.code += "infer_type(" + f_name + ", " + input_types_dict + ", " + f_name + "_typecache)\n"

        GenerateIRFile(f_name, vis.code, os.path.dirname(os.path.abspath(file_name))+"/")
        mod = SourceFileLoader(f_name, os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py").load_module()
        
        all_functions_out_types[f_name] = (getattr(mod, f_name + "_typecache")[getattr(mod, "results").id])

        out_type = all_functions_out_types[f_name]
        if type(out_type) == IntType:
            out_type = "int"
        elif type(out_type) == FloatType:
            out_type = "float"
        elif type(out_type) == DictionaryType:
            out_type = "DictionaryType()"
            abbrOutType = getAbbrDictName(all_functions_out_types[f_name].fromType, BoolType())

        vis.code += "def get_code(db_code):\n\treturn gen.FinalizeCPPCodeNoFile(gen.GenerateCPPCode(" + f_name + ", " + f_name + "_typecache, True, " + str(cores) + "), " + out_type + ", db_code, " + str(is_columnar_input) + ", " +  str(is_fast_dict_input) + ", \"" + abbrOutType + "\", \"" + file_name[:-3] + "\")"

        GenerateIRFile(f_name, vis.code, os.path.dirname(os.path.abspath(file_name))+"/")
        mod = SourceFileLoader(f_name, os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py").load_module()
        RemoveFile(os.path.dirname(os.path.abspath(file_name)) + "/" + f_name + ".py")

        cppcode += "static PyObject * " + f_name + "(PyObject *self, PyObject* args)\n{\n"
        
        db_code = ""

        if is_columnar_input:
            db_code += "const static int numpy_initialized =  init_numpy();\n\n\n"

        if is_fast_dict_input:
            db_code += "\n\n"

            counter = 0
            for type_var_name in sdql_decorator_node_in_type.values:        
                in_type_node = searchVis.findNode(node, lambda n:
                        type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == type_var_name.id).value

                input_vals = in_type_node.keys[0].args[0].values
                
                abbr=""
                for v in input_vals:
                    if type(v) == ast.Call and v.func.id == "string":
                        abbr += "s" + str(v.args[0].value)
                    elif v.id == "date":
                        abbr += "i"   
                    elif v.id == "int":
                        abbr += "i"
                    elif v.id == "float":
                        abbr += "f"
                    elif v.id == "bool":
                        abbr += "b"
                    else:
                        print("Error: input type not defined!")
                abbr = "FastDict_" + abbr + "_b"
                db_code += "\tauto& " + sdql_decorator_node_in_type.keys[counter].value + " = *(((" + abbr + "*)PyDict_GetItemString(db_, \"" + sdql_decorator_node_in_type.keys[counter].value + "\"))->dict);\n"
                counter += 1

            db_code += "\n\n"
        else:
            if is_columnar_input:
                counter = 0
                for type_var_name in sdql_decorator_node_in_type.values:        
                    in_type_node = None
                    in_type_node = searchVis.findNode(node, lambda n:
                            type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == type_var_name.id).value
                    db_code += "\tauto " + sdql_decorator_node_in_type.keys[counter].value + "_size = PyArray_Size(PyList_GetItem(PyList_GetItem(db_, " + str(counter) + "), 0));\n"
                    db_code += "\t" + mapping(sdql_decorator_node_in_type.keys[counter].value) + "_size = " + sdql_decorator_node_in_type.keys[counter].value + "_size;\n"

                    input_keys = in_type_node.keys[0].args[0].keys
                    input_vals = in_type_node.keys[0].args[0].values
                    counter2 = 0
                    for k in input_keys:

                        if type(input_vals[counter2]) == ast.Call and input_vals[counter2].func.id == "string":
                            db_code += "\tauto " + k.value + "= (VarChar<" + str(input_vals[counter2].args[0].value if len(input_vals[counter2].args) else 25) + ">*)PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, " + str(counter) + "), " + str(counter2) + "));\n"
                            counter2 += 1
                            continue

                        db_code += "\tauto " + k.value + " = ("
                        if input_vals[counter2].id == "date":
                            db_code += "long*"   
                        elif input_vals[counter2].id == "int":
                            db_code += "long*"
                        elif input_vals[counter2].id == "float":
                            db_code += "double*"
                        elif input_vals[counter2].id == "bool":
                            db_code += "bool*"
                        else:
                            print("Error: input type not defined!")
                        db_code += ")PyArray_DATA(PyList_GetItem(PyList_GetItem(db_, " + str(counter) + "), " + str(counter2) + "));\n"
                        counter2 += 1           

                    db_code += "\n"
                    counter += 1
            else:
                counter = 0
                for type_var_name in sdql_decorator_node_in_type.values:        
                    if counter>0:
                        db_code += "\t"

                    db_code += "phmap::flat_hash_map<tuple<" 
                    
                    in_type_node = None
                    in_type_node = searchVis.findNode(node, lambda n: type(n) == ast.Assign and type(n.targets[0]) == ast.Name and n.targets[0].id == type_var_name.id).value
                    input_keys = in_type_node.keys[0].args[0].keys
                    input_vals = in_type_node.keys[0].args[0].values
                    counter2 = 0

                    for k in input_keys:
                        if type(input_vals[counter2]) == ast.Call and input_vals[counter2].func.id == "string":
                            db_code += "VarChar<" + str(input_vals[counter2].args[0].value if len(input_vals[counter2].args) else 25) + ">, "
                            counter2 += 1
                            continue
                        if input_vals[counter2].id == "date":
                            db_code += "long"   
                        elif input_vals[counter2].id == "int":
                            db_code += "long"
                        elif input_vals[counter2].id == "float":
                            db_code += "double"
                        elif input_vals[counter2].id == "bool":
                            db_code += "bool"
                        else:
                            print("Error: input type not defined!")
                        
                        db_code += ", "
                        counter2 += 1

                    db_code = db_code[:-2]
                    db_code += ">, bool> " + sdql_decorator_node_in_type.keys[counter].value + ";\n"
                    db_code += "\tauto " + sdql_decorator_node_in_type.keys[counter].value + "_py = PyDict_Keys(PyObject_GetAttr(PyDict_GetItemString(db_, \"" + sdql_decorator_node_in_type.keys[counter].value + "\"), PyUnicode_FromString(\"_sr_dict__container\")));"
                    
                    db_code += "\n\tauto " + sdql_decorator_node_in_type.keys[counter].value + "_py_size = PyList_Size(" + sdql_decorator_node_in_type.keys[counter].value + "_py" + ");"
                    db_code += "\n\tfor (int i=0; i< " + sdql_decorator_node_in_type.keys[counter].value + "_py_size; i++)\n\t{\n"
                    db_code += "\t\tauto tmpRec = PyObject_GetAttr(PyList_GetItem(" + sdql_decorator_node_in_type.keys[counter].value + "_py, i), PyUnicode_FromString(\"_sr_dict__container\"));\n"
                    db_code += "\t\tauto tmpRecKeys = PyDict_Keys(tmpRec);\n"
                    db_code += "\t\tauto tmpRecVals = PyDict_Values(tmpRec);\n"
                    db_code += "\n\t\tauto key_size = PyList_Size(tmpRecKeys);\n"

                    counter3 = 0              

                    db_code += "\t\t" + sdql_decorator_node_in_type.keys[counter].value + "[make_tuple("
                    
                    for val in input_vals:                
                        if type(val) == ast.Call and val.func.id == "string":
                            db_code += "VarChar<" + str(val.args[0].value if len(val.args) else 25) + ">(PyUnicode_AsWideCharString(PyList_GetItem(tmpRecVals, " + str(counter3) + "), NULL)), "
                            counter3 += 1
                            continue                
                        if val.id == "date":
                            db_code += "PyLong_AsLong"   
                        elif val.id == "int":
                            db_code += "PyLong_AsLong"
                        elif val.id == "float":
                            db_code += "PyFloat_AsDouble"
                        else:
                            print("Error: input type not defined!")
                    
                        db_code += "(PyList_GetItem(tmpRecVals, " + str(counter3) + ")), "
                        counter3+=1

                    db_code = db_code[:-2] + ")] = true;"

                    db_code += "\n\t}\n\n" 

                    counter += 1
                db_code += "\n\n"
            
        cppcode += mod.get_code(db_code) + "\n"
        cppcode += "}\n\n"


    mod_name = file_name.split("/")[-1].split(".")[0] + "_compiled"
    
    cppcode += """
static PyMethodDef """ + mod_name + """_methods[] = {\n"""

    for f in functions:
        cppcode += "{\"" + f.name + "_compiled\", " + f.name + ", METH_VARARGS, \"\"},\n"

    cppcode += """
{NULL,		NULL}		/* sentinel */
};

///////////////////////////////////////////////////////////////////////

static char module_docstring[] = "";

static struct PyModuleDef """ + mod_name + """ = 
{
    PyModuleDef_HEAD_INIT,
    """ + "\"" + mod_name + "\"" + """,
    module_docstring,
    -1,
    """ + mod_name + """_methods
};

PyMODINIT_FUNC PyInit_""" + mod_name + """(void) 
{
    return PyModule_Create(&""" + mod_name + """);
}

int main(int argc, char **argv)
{
	Py_SetProgramName((wchar_t*)argv[0]);
	Py_Initialize();
	PyInit_""" + mod_name + """();
	Py_Exit(0);
}"""
    


    output_dicts = {}

    for k,v in all_functions_out_types.items():
        if type(v)==DictionaryType:
            output_dicts[k] = v


    abbrCache = []
    tmp = {}
    for k,v in output_dicts.items():
        tmp_v = v.fromType
        if getAbbrDictName(tmp_v, BoolType()) not in abbrCache:
            abbrCache.append(getAbbrDictName(tmp_v, BoolType()))
            tmp[k]=v

    cppcode_first += getAllDictDefinitions(tmp) + "\n"
    cppcode_first += cppcode + "\n"
    cppcode = cppcode_first 

    CreateCPPFile(cppcode, file_name[:-3], os.path.dirname(os.path.abspath(file_name))+"/")

    generate_fastdicts(tmp, file_name, file_dir) 
    
################################################################################

if __name__ == "__main__":
    main()