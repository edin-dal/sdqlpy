from typing import List, NoReturn, Tuple
from enum import Enum
from sdql_ir import *


def infer_type(AST: Expr, context: dict, cache: dict):
    inputType = type(AST)
    
    if inputType == ConstantExpr:
        if type(AST.type) == IntType:
            cache[AST.id] = IntType()
            return cache[AST.id]
        elif type(AST.type) == FloatType:
            cache[AST.id] = FloatType()
            return cache[AST.id]
        if type(AST.type) == BoolType:
            cache[AST.id] = BoolType()
            return cache[AST.id]
        if type(AST.type) == StringType:
            cache[AST.id] = StringType()
            return cache[AST.id]

    elif inputType == DicConsExpr:
        for e in AST.exprList:
            infer_type(e, context, cache)
        cache[AST.id] = DictionaryType(infer_type(AST.exprList[0], context, cache), infer_type(AST.exprList[1], context, cache))
        return cache[AST.id]

    elif inputType == EmptyDicConsExpr:
        cache[AST.id] = DictionaryType()
        return cache[AST.id]

    elif inputType == RecAccessExpr:
        cache[AST.id] = infer_type(AST.recExpr, context, cache).typesDict[AST.name]
        return cache[AST.id]

    elif inputType == IfExpr:
        infer_type(AST.condExpr, context, cache)
        thenType = infer_type(AST.thenBodyExpr, context, cache)
        elseType = infer_type(AST.elseBodyExpr, context, cache)

        zeroCheck = return_non_zero_dic_and_rec(thenType, elseType)
        if zeroCheck != None:
            cache[AST.id] = zeroCheck
            return cache[AST.id]
        # if thenType != elseType:
        #     print("Error: if type cannot be deduced!")
        #     return
        cache[AST.id] = thenType
        return cache[AST.id]

    elif inputType == AddExpr:
        op1 = infer_type(AST.op1Expr, context, cache)
        op2 = infer_type(AST.op2Expr, context, cache)
        zeroCheck = return_non_zero_dic_and_rec(op1, op2)
        if zeroCheck != None:
            cache[AST.id] = zeroCheck
            return cache[AST.id]
        if op1 != op2:
            print("Error: add type cannot be deduced! | " + str(op1) + " + " + str(op2) + " | " + op1.name + " | " + op2.name)
            return
        cache[AST.id] = op1
        return cache[AST.id]

    elif inputType == SubExpr:
        op1 = infer_type(AST.op1Expr, context, cache)
        op2 = infer_type(AST.op2Expr, context, cache)
        zeroCheck = return_non_zero_dic_and_rec(op1, op2)
        if zeroCheck != None:
            cache[AST.id] = zeroCheck
            return cache[AST.id]
        if op1 != op2:
            print("Error: sub type cannot be deduced! | " + str(op1) + " - " + str(op2) + " | " + op1.name + " | " + op2.name)
            return
        cache[AST.id] = op1
        return cache[AST.id]

    elif inputType == MulExpr:
        op1 = infer_type(AST.op1Expr, context, cache)
        op2 = infer_type(AST.op2Expr, context, cache)
        zeroCheck = return_non_zero_dic_and_rec(op1, op2)
        if zeroCheck != None:
            cache[AST.id] = zeroCheck
            return cache[AST.id]
        if op1 != op2:
            print("Error: mul type cannot be deduced! | " + str(op1) + " * " + str(op2) + " | " + op1.name + " | " + op2.name)
            return
        cache[AST.id] = op1
        return cache[AST.id]

    elif inputType == DivExpr:
        op1 = infer_type(AST.op1Expr, context, cache)
        op2 = infer_type(AST.op2Expr, context, cache)
        zeroCheck = return_non_zero_dic_and_rec(op1, op2)
        if zeroCheck != None:
            cache[AST.id] = zeroCheck
            return cache[AST.id]
        if op1 != op2:
            print("Error: div type cannot be deduced!")
            return
        cache[AST.id] = op1
        return cache[AST.id]

    elif inputType == DicLookupExpr:
        infer_type(AST.keyExpr, context, cache)
        cache[AST.id] = infer_type(AST.dicExpr, context, cache).toType
        return cache[AST.id]

    elif inputType == LetExpr:
        context[AST.varExpr.name] = infer_type(AST.valExpr, context, cache)
        cache[AST.id] = infer_type(AST.bodyExpr, context, cache)
        context.pop(AST.varExpr.name, None)
        return cache[AST.id]

    elif inputType == VarExpr:
        cache[AST.id] = context[AST.name]
        return cache[AST.id]

    elif inputType == PromoteExpr:
        infer_type(AST.bodyExpr, context, cache)
        cache[AST.id] = AST.toType
        return cache[AST.id]

    elif inputType == CompareExpr:
        cache[AST.leftExpr.id] = infer_type(AST.leftExpr, context, cache)
        cache[AST.rightExpr.id] = infer_type(AST.rightExpr, context, cache)
        cache[AST.id] = BoolType()
        return cache[AST.id]

    elif inputType == ExtFuncExpr:
        infer_type(AST.inp1, context, cache)
        infer_type(AST.inp2, context, cache)
        infer_type(AST.inp3, context, cache)
        if AST.symbol == ExtFuncSymbol.StringContains:
            cache[AST.id] = BoolType()
            return cache[AST.id]
        if AST.symbol == ExtFuncSymbol.SubStr:           
            cache[AST.id] = StringType(1 + AST.inp3.value - AST.inp2.value)
            return cache[AST.id]
        if AST.symbol == ExtFuncSymbol.ToStr:
            cache[AST.id] = StringType()
            return cache[AST.id]
        if AST.symbol == ExtFuncSymbol.ExtractYear:
            cache[AST.id] = IntType()
            return cache[AST.id]
        if AST.symbol == ExtFuncSymbol.StartsWith:
            cache[AST.id] = BoolType()
            return cache[AST.id]
        if AST.symbol == ExtFuncSymbol.EndsWith:
            cache[AST.id] = BoolType()
            return cache[AST.id]
        if AST.symbol == ExtFuncSymbol.DictSize:
            cache[AST.id] = IntType()
            return cache[AST.id]
        if AST.symbol == ExtFuncSymbol.FirstIndex:
            cache[AST.id] = IntType()
            return cache[AST.id]
        print("Error: Unknown ExtFuncSymbol: " + str(AST.symbol))
        return

    elif inputType == PairAccessExpr:
        cache[AST.id] = infer_type(AST.pairExpr, context, cache)[AST.index]
        return cache[AST.id]

    elif inputType == SumExpr:
        cache[AST.dictExpr.id] = infer_type(AST.dictExpr, context, cache)
        dictType = cache[AST.dictExpr.id]
        if dictType != VectorType():
            cache[AST.id] = infer_type(LetExpr(AST.varExpr, (dictType.fromType, dictType.toType), AST.bodyExpr), context, cache)
        else:
            context[AST.varExpr.name] = dictType.exprTypes[0]
            cache[AST.id] = infer_type(AST.bodyExpr, context, cache)
            context.pop(AST.varExpr.name)
        return cache[AST.id]

    elif inputType == RecConsExpr:
        tmpList = []
        for (k, v) in AST.initialPairs:
            tmpList.append((k, infer_type(v, context, cache)))
        cache[AST.id] = RecordType(tmpList)
        return cache[AST.id]

    elif inputType == VecConsExpr:
        tmpList = []
        for e in AST.exprList:
            tmpList.append(infer_type(e, context, cache))
        cache[AST.id] = VectorType(tmpList)
        return cache[AST.id]
        

    # elif inputType == HashJoinEqualExpr:
    #     # Must be revised completely + cache types
    #     cache[AST.id] = hashequaljoin(infer_type(AST.dic1, context, cache), infer_type(
    #         AST.dic2, context, cache), "", "", AST.col1, AST.col2)
    #     return cache[AST.id]

    elif inputType == ConcatExpr:
        tmp = []
        rec1 = infer_type(AST.rec1, context, cache)
        rec2 = infer_type(AST.rec2, context, cache)
        for (k, v) in rec1.typesList:
            tmp.append((k, v))
        for (k, v) in rec2.typesList:
            tmp.append((k, v))
        cache[AST.id] = RecordType(tmp)
        return cache[AST.id]

    elif inputType == tuple:
        return AST

    elif inputType != Expr:
        print("Error: Unknown AST: " + str(type(AST)) + "| Value: " + str(AST))
        return


def return_non_zero_dic_and_rec(inp1, inp2):
    if type(inp1) == DictionaryType and type(inp2) == DictionaryType:
        if inp1.fromType is not None and inp1.toType is not None:
            return inp1
        else:
            return inp2

    if type(inp1) == RecordType and type(inp2) == RecordType:
        if inp1.typesList is not None:
            return inp1
        else:
            return inp2

    return None
############################################################