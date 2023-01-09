import os
from sdql import *
from sdql_ir import *

def GenerateSDQLPYCode(AST: Expr, cache: dict, isFirstCall=False):

    inputType = type(AST)

    if inputType == ConstantExpr:
        print(inputType)
    elif inputType == RecAccessExpr:
        print(inputType)
    elif inputType == IfExpr:
        print(inputType) 
    elif inputType == AddExpr:
        print(inputType)
    elif inputType == SubExpr:
        print(inputType)
    elif inputType == MulExpr:
        print(inputType)
    elif inputType == DivExpr:
        print(inputType)
    elif inputType == DicLookupExpr:
        print(inputType)
    elif inputType == LetExpr:
        print(inputType)
    elif inputType == VarExpr:
        print(inputType)
    elif inputType == PairAccessExpr:
        print(inputType)
    elif inputType == PromoteExpr:
        print(inputType)
    elif inputType == CompareExpr:
        print(inputType)
    elif inputType == SumExpr:          
        print(inputType)
    elif inputType == DicConsExpr:
        print(inputType)
    elif inputType == EmptyDicConsExpr:
        print(inputType)
    elif inputType == RecConsExpr:
        print(inputType)
    elif inputType == VecConsExpr:
        print(inputType)
    elif inputType == ConcatExpr:
        print(inputType)
    elif inputType == ExtFuncExpr:
        print(inputType)
    else:
        print("Error: Unknown AST: " + str(type(AST)))
        return
######################################################