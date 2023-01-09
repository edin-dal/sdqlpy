from typing import List, Tuple
from enum import Enum
from sdql import *

# Type Definitions #########################################


class Type:
    def __init__(self) -> None:
        pass

    def __eq__(self, other):
        if type(self) == type(other):
            return True
        else:
            return False


class IntType(Type):
    def __init__(self) -> None:
        pass


class FloatType(Type):
    def __init__(self) -> None:
        pass


class BoolType(Type):
    def __init__(self) -> None:
        pass


class StringType(Type):
    def __init__(self, charCount=None) -> None:
        self.charCount = charCount


class DictionaryType(Type):
    def __init__(self, fromType: Type=None, toType: Type=None):
        super().__init__()
        self.fromType = fromType
        self.toType = toType
        self.type_name = None

    def __eq__(self, other):
        if other is None or other == VectorType():
            return False
        if (self.fromType is None and self.toType is None) or (other.fromType is None and other.toType is None):
            return True
        if self.fromType == other.fromType and self.toType == other.toType:
            return True
        else:
            return False


class RecordType(Type):
    def __init__(self, pairTypes: List['Tuple[str,Type]']=None):
        super().__init__()
        if pairTypes is None:
            self.typesDict = dict([])
            self.typesList = []       
        self.typesDict = dict(pairTypes)
        self.typesList = pairTypes

    def __eq__(self, other):
        if self is None and other is None:
            return True
        if (self is None and other is not None) or (self is not None and other is None):
            return False
        if (type(other) != RecordType):
            return False
        if self.typesList is None or other.typesList is None:
            return True
        if len(self.typesList) != len(other.typesList):
            return False
        for (k, v) in self.typesList:
            if other.typesList[k] is None or other.typesList[k] != v:
                return False
        return True


class VectorType(Type):
    def __init__(self, exprTypes: List['Type']=None):
        super().__init__()
        if exprTypes is None:
            self.exprTypes = []       
        self.exprTypes = exprTypes

    def __eq__(self, other):
        if self is None and other is None:
            return True
        if (self is None and other is not None) or (self is not None and other is None):
            return False
        if VectorType == type(other):
            return True
        else:
            return False


class CompareSymbol(Enum):
    EQ = 1
    LT = 2
    GT = 3
    LTE = 4
    GTE = 5
    NE = 6


class ExtFuncSymbol(Enum):
    StringContains = 1
    SubStr = 2
    ToStr = 3
    ExtractYear = 4
    StartsWith = 5
    EndsWith = 6
    DictSize = 7
    FirstIndex = 8

# IR Definitions ###########################################


class Expr():
    exprList = []
    id = None
    parent = None

    def __init__(self, exprs: List['Expr']):
        self.exprList = exprs
        self.id = newExprID()

    def __add__(self, other):
        return AddExpr(self, other)

    def __sub__(self, other):
        return SubExpr(self, other)

    def __mul__(self, other):
        return MulExpr(self, other)

    def __truediv__(self, other):
        return DivExpr(self, other)

    def __floordiv__(self, other):
        return DivExpr(self, other)

    def __getattr__(self, key):
        if type(self) == PairAccessExpr or type(self) == RecConsExpr or type(self) == DicLookupExpr or type(self)==VarExpr:
            return RecAccessExpr(self, key)

    def __getitem__(self, key):
        if issubclass(type(key), Expr):
            return DicLookupExpr(self, key)
        elif type(key) == int:
            return PairAccessExpr(self, key)
        else:
            print("Error: get element not supported for self: " +
                  str(type(self)) + " and key:" + str(type(key)))

    def __eq__(self, other):
        return CompareExpr(CompareSymbol.EQ, self, other)

    def __ne__(self, other):
        return CompareExpr(CompareSymbol.NE, self, other)

    def __lt__(self, other):
        return CompareExpr(CompareSymbol.LT, self, other)

    def __le__(self, other):
        return CompareExpr(CompareSymbol.LTE, self, other)

    def __gt__(self, other):
        return CompareExpr(CompareSymbol.GT, self, other)

    def __ge__(self, other):
        return CompareExpr(CompareSymbol.GTE, self, other)

    def __hash__(self):
        return hash((str(self)))


class ConstantExpr(Expr):
    type = None

    def __init__(self, value):
        super().__init__(None)
        self.value = value
        if (type(value) == int):
            self.type = IntType()
        elif (type(value) == float):
            self.type = FloatType()
        elif (type(value) == bool):
            self.type = BoolType()
        elif (type(value) == str):
            self.type = StringType()
        elif value == None:
            self.type = None
        else:
            print("Error: constant type not supported!")

    def printInnerVals(self):
        return " | " + "value: " + str(self.value) + " | " + "valueType: " + str(self.type)


class VarExpr(Expr):
    def __init__(self, varName: str):
        super().__init__([])
        self.name: str = varName
        self.datasetVarName = None

    def printInnerVals(self):
        return " | " + "name: " + str(self.name)


class LetExpr(Expr):
    def __init__(self, varExpr: VarExpr, valExpr: Expr, bodyExpr: Expr):
        super().__init__([varExpr, valExpr, bodyExpr])
        self.varExpr = varExpr
        self.valExpr = valExpr
        self.bodyExpr = bodyExpr


class SumExpr(Expr):
    def __init__(self, varExpr: VarExpr, dictExpr: Expr, bodyExpr: Expr, isAssignmentSum=False, dictType="phmap::flat_hash_map"):
        super().__init__([varExpr, dictExpr, bodyExpr])

        self.outputExpr = VarExpr(newVarName())
        self.varExpr = varExpr
        self.dictExpr = dictExpr
        self.bodyExpr = bodyExpr
        self.isAssignmentSum = isAssignmentSum
        self.dictType = dictType
        self.exprList.append(self.outputExpr)
        if self.bodyExpr == EmptyDicConsExpr:
            self.bodyExpr.parent = self


class DicConsExpr(Expr):
    def __init__(self, initialPairs: List[Tuple[Expr, Expr]]):
        super().__init__([])
        self.fromType = None
        self.toType = None
        self.exprList = []
        self.initialPairs = initialPairs
        for p in initialPairs:
            self.exprList.append(p[0])
            self.exprList.append(p[1])


class EmptyDicConsExpr(Expr):
    def __init__(self):
        super().__init__(None)
        self.fromType = None
        self.toType = None
        self.exprList = []
        self.initialPairs = []

    def printInnerVals(self):
        return " | " + "fromType: " + str(self.fromType) + " | " + "toType: " + str(self.toType)


class DicLookupExpr(Expr):
    def __init__(self, dicExpr: Expr, keyExpr: Expr):
        super().__init__([dicExpr, keyExpr])
        self.dicExpr = dicExpr
        self.keyExpr = keyExpr


class RecConsExpr(Expr):
    def __init__(self, initialPairs: List[Tuple[str, Expr]]):
        super().__init__([])
        self.initialPairs = initialPairs
        for p in initialPairs:
            self.exprList.append(p[1])


class VecConsExpr(Expr):
    def __init__(self, initialExprs: List[Expr]):
        super().__init__([])
        for e in initialExprs:
            self.exprList.append(e)


class RecAccessExpr(Expr):
    def __init__(self, recExpr: Expr, fieldName: str):
        super().__init__([recExpr])
        self.name: str = fieldName
        self.recExpr = recExpr

    def printInnerVals(self):
        return " | " + "name: " + str(self.name)


class IfExpr(Expr):
    def __init__(self, condExpr: Expr, thenBodyExpr: Expr, elseBodyExpr: Expr):
        super().__init__([condExpr, thenBodyExpr, elseBodyExpr])
        self.condExpr = condExpr
        self.thenBodyExpr = thenBodyExpr
        self.elseBodyExpr = elseBodyExpr
        self.additionVarExpr = None
        self.isInNonParallelSum = False
        if self.elseBodyExpr == EmptyDicConsExpr:
            self.elseBodyExpr.parent = self


class AddExpr(Expr):
    def __init__(self, op1Expr: Expr, op2Expr: Expr):
        super().__init__([op1Expr, op2Expr])
        self.op1Expr = op1Expr
        self.op2Expr = op2Expr


class SubExpr(Expr):
    def __init__(self, op1Expr: Expr, op2Expr: Expr):
        super().__init__([op1Expr, op2Expr])
        self.op1Expr = op1Expr
        self.op2Expr = op2Expr


class MulExpr(Expr):
    def __init__(self, op1Expr: Expr, op2Expr: Expr):
        super().__init__([op1Expr, op2Expr])
        self.op1Expr = op1Expr
        self.op2Expr = op2Expr


class DivExpr(Expr):
    def __init__(self, op1Expr: Expr, op2Expr: Expr):
        super().__init__([op1Expr, op2Expr])
        self.op1Expr = op1Expr
        self.op2Expr = op2Expr


class PromoteExpr(Expr):
    def __init__(self, fromType: Type, toType: Type, bodyExpr: Expr):
        super().__init__([bodyExpr])
        self.fromType = fromType
        self.toType = toType
        self.bodyExpr = bodyExpr

    def printInnerVals(self):
        return " | " + "fromType: " + str(self.fromType) + " | " + "toType: " + str(self.toType)


class CompareExpr(Expr):
    def __init__(self, compareType: CompareSymbol, leftExpr: Expr, rightExpr: Expr):
        super().__init__([leftExpr, rightExpr])
        self.compareType = compareType
        self.leftExpr = leftExpr
        self.rightExpr = rightExpr

        self.leftExpr.parent = self
        self.rightExpr.parent = self

    def printInnerVals(self):
        return " | " + "compareType: " + str(self.compareType)


class PairAccessExpr(Expr):
    def __init__(self, pairExpr: Expr, index: int):
        super().__init__([pairExpr])
        self.index = index
        self.pairExpr = pairExpr

    def printInnerVals(self):
        return " | " + "index: " + str(self.index)


class ConcatExpr(Expr):
    def __init__(self, rec1: Expr, rec2: Expr):
        super().__init__([rec1, rec2])
        self.rec1 = rec1
        self.rec2 = rec2


class ExtFuncExpr(Expr):
    def __init__(self, symbol: ExtFuncSymbol, inp1: Expr, inp2: Expr=None, inp3: Expr=None):
        super().__init__([inp1, inp2, inp3])
        self.symbol = symbol
        self.inp1 = inp1
        self.inp2 = inp2
        self.inp3 = inp3

    def printInnerVals(self):
        return " | " + "type: " + str(self.symbol)

# AST Helper Functions #####################################

def newExprID():
    if not hasattr(newExprID, "counter"):
        newExprID.counter = 0
    newExprID.counter += 1
    return newExprID.counter

def newVarName():
    if not hasattr(newVarName, "counter"):
        newVarName.counter = 0
    newVarName.counter += 1
    return "v" + str(newVarName.counter)

def newFuncName():
    if not hasattr(newFuncName, "counter"):
        newFuncName.counter = 0
    newFuncName.counter += 1
    return "f" + str(newFuncName.counter)

def SumBuilder(func, dictExpr: Expr, isAssignmentSum=False, dictType="phmap::flat_hash_map"):
    tmpVar = VarExpr(newVarName())
    return SumExpr(tmpVar, dictExpr, func(tmpVar), isAssignmentSum, dictType)

def LetBuilder(valExpr, bodyExprFunc):
    tmpVar = VarExpr(newVarName())
    return LetExpr(tmpVar, valExpr, bodyExprFunc(tmpVar))

def SetBuilder(exprList: List[Expr]):
    tmpList = []
    if len(exprList) > 0:
        for e in exprList:
            tmpList.append((e, ConstantExpr(True)))
        return DicConsExpr(tmpList)
    else:
        return EmptyDicConsExpr()

def JoinPartitionBuilder(dict, partitionColumn, filterFunc, outputColumns, dictType="phmap::flat_hash_map"):
    
    def outputCols(rec):
        tmpList = []
        if outputColumns == []:
            tmpList.append((partitionColumn, RecAccessExpr(rec, partitionColumn)))
        else:
            for col in outputColumns:
                tmpList.append((col, RecAccessExpr(rec, col)))
        return RecConsExpr(tmpList)

    def finalSelector(rec):
        if filterFunc is not None:
            return IfExpr(filterFunc(rec), DicConsExpr([(RecAccessExpr(rec, partitionColumn), outputCols(rec))]), EmptyDicConsExpr())
        else:
            return DicConsExpr([(RecAccessExpr(rec, partitionColumn), outputCols(rec))])

    return SumBuilder(lambda x: finalSelector(x[0]), dict, True, dictType)

def JoinProbeBuilder(partitionedLeft, right, probeColumn, filterFunc, finalizationFunc, isAssignmentSum=False, dictType="phmap::flat_hash_map"):
    probeVar = VarExpr(newVarName())
    
    def finalSelector(rec):
        tmpPKey = RecAccessExpr(rec, probeColumn)
        probeResult = probeVar[tmpPKey]
        if filterFunc is not None:
            return IfExpr(filterFunc(rec), IfExpr(probeResult != ConstantExpr(None), finalizationFunc(probeVar[tmpPKey], rec), EmptyDicConsExpr()),EmptyDicConsExpr())
        else:
            return IfExpr(probeResult != ConstantExpr(None), finalizationFunc(probeVar[tmpPKey], rec), EmptyDicConsExpr())

    return LetExpr(probeVar, partitionedLeft, SumBuilder(lambda x: finalSelector(x[0]), right, isAssignmentSum, dictType))

############################################################