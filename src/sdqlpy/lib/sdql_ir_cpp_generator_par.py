import os
from sdql import *
from sdql_ir import *

isInsideBodyOfDatasetReaderSum = None
datasetReaderSumIdxVar = None
outputDictionariesInitializationCode = ""
isInRecConsExpr = False
globalCoresNo = 1
isInsideBodyOfSum = False

def GenerateCPPCode(AST: Expr, cache: dict, isFirstCall=False, coresNo=1):
    global isInsideBodyOfDatasetReaderSum 
    global datasetReaderSumIdxVar
    global outputDictionariesInitializationCode
    global isInRecConsExpr
    global globalCoresNo
    global isInsideBodyOfSum

    if isFirstCall:
        globalCoresNo = coresNo

    inputType = type(AST)

    if inputType == ConstantExpr:
        if type(AST.type) == StringType:
            # return "\"" + AST.value + "\""
            return "ConstantString(\"" + AST.value + "\", " + str(1+len(AST.value)) + ")"
        elif type(AST.type) == IntType:
            return "(long)" + str(AST.value)
        else:
            return str(AST.value).lower()

    elif inputType == RecAccessExpr:
        selectedIdx = -1
        for idx, val in enumerate(cache[AST.recExpr.id].typesList):
            if val[0] == AST.name:
                selectedIdx = idx
                break
        if (selectedIdx == -1):
            print("Error: RecAccessExpr access index is invalid!")
            return None
        # if type(AST.recExpr)!=DicLookupExpr and ((isInsideBodyOfDatasetReaderSum != None) and ((isInsideBodyOfDatasetReaderSum == "partsupp" and AST.name[0,1] == "ps") or (isInsideBodyOfDatasetReaderSum[0] == (AST.name)[0]))):
        if type(AST.recExpr)!=DicLookupExpr and (isInsideBodyOfDatasetReaderSum != None) and (AST.name in list(list(zip(*(cache[AST.recExpr.id].typesList)))[0])):
            return AST.name + "[" + datasetReaderSumIdxVar + "]"
        else:
            return " /* " + AST.name + " */" + "get<" + str(selectedIdx) + ">(" + GenerateCPPCode(AST.recExpr, cache) + ")"

    elif inputType == IfExpr:

        condCode = GenerateCPPCode(AST.condExpr, cache)

        if isInRecConsExpr:
            return "(" + condCode + ") ? (" + GenerateCPPCode(AST.thenBodyExpr, cache) + ") : (" +  GenerateCPPCode(AST.elseBodyExpr, cache) + ")"

        if AST.additionVarExpr is None:
            return "if (" + condCode + "){ " + GenerateCPPCode(AST.thenBodyExpr, cache) + ";}"
        else:
            if type(AST.thenBodyExpr) == IfExpr:
                AST.thenBodyExpr.additionVarExpr = AST.additionVarExpr
                AST.thenBodyExpr.isAssignmentSum = AST.isAssignmentSum
                AST.thenBodyExpr.dictType = AST.dictType if AST.dictType != None else ""
                return "if (" + condCode + "){" + GenerateCPPCode(AST.thenBodyExpr, cache) + "}"
            else:
                return "if (" + condCode + "){" + AdditionCodeGenerator(AST.additionVarExpr, AST.thenBodyExpr, cache, AST.isAssignmentSum, AST.isInNonParallelSum, AST.dictType) + "}"
 
    elif inputType == AddExpr:
        #if (type(AST.op1Expr) == CompareExpr or (type(AST.op1Expr) == ConstantExpr and AST.op1Expr.type == BoolType)) and (type(AST.op2Expr) == CompareExpr or (type(AST.op2Expr) == ConstantExpr and AST.op2Expr.type == BoolType)):
        if cache[AST.op1Expr.id]==BoolType() and cache[AST.op2Expr.id]==BoolType():
            return "(" + GenerateCPPCode(AST.op1Expr, cache) + " || " + GenerateCPPCode(AST.op2Expr, cache) + ")"
        return "(" + GenerateCPPCode(AST.op1Expr, cache) + " + " + GenerateCPPCode(AST.op2Expr, cache) + ")"

    elif inputType == SubExpr:
        return "(" + GenerateCPPCode(AST.op1Expr, cache) + " - " + GenerateCPPCode(AST.op2Expr, cache) + ")"

    elif inputType == MulExpr:
        ##if (type(AST.op1Expr) == CompareExpr or type(AST.op1Expr) == MulExpr or (type(AST.op1Expr) == ConstantExpr and AST.op1Expr.type == BoolType)) and (type(AST.op2Expr) == CompareExpr or type(AST.op2Expr) == MulExpr or (type(AST.op2Expr) == ConstantExpr and AST.op2Expr.type == BoolType)):
        if cache[AST.op1Expr.id]==BoolType() and cache[AST.op2Expr.id]==BoolType():
            return "(" + GenerateCPPCode(AST.op1Expr, cache) + " && " + GenerateCPPCode(AST.op2Expr, cache) + ")"
        return "(" + GenerateCPPCode(AST.op1Expr, cache) + " * " + GenerateCPPCode(AST.op2Expr, cache) + ")"

    elif inputType == DivExpr:
        return "(" + GenerateCPPCode(AST.op1Expr, cache) + " / " + GenerateCPPCode(AST.op2Expr, cache) + ")"

    elif inputType == DicLookupExpr:
        if type(AST.parent) == CompareExpr and (AST.parent.compareType==CompareSymbol.EQ or AST.parent.compareType==CompareSymbol.NE) and ((type(AST.parent.rightExpr) == ConstantExpr and AST.parent.rightExpr.value == None) or (type(AST.parent.leftExpr) == ConstantExpr and AST.parent.leftExpr.value == None)):
 
            if cache[AST.dicExpr.id].type_name == "dense_array":
                return "(" + GenerateCPPCode(AST.dicExpr, cache) + "[" + GenerateCPPCode(AST.keyExpr, cache) + "] != " + GenerateCPPZeroOfType(cache[AST.dicExpr.id].toType) + ")" 
            else:
                return "(" + GenerateCPPCode(AST.dicExpr, cache) + ").contains(" + GenerateCPPCode(AST.keyExpr, cache) + ")" 
        else:
            if cache[AST.dicExpr.id].type_name == "dense_array":
                return GenerateCPPCode(AST.dicExpr, cache) + "[" + GenerateCPPCode(AST.keyExpr, cache) + "]" 
            else:
                return "(" + GenerateCPPCode(AST.dicExpr, cache) + ").at(" + GenerateCPPCode(AST.keyExpr, cache) + ")" 

    elif inputType == LetExpr:
        if type(AST.valExpr) in [SumExpr, LetExpr]:
            finalVarName = None
            if type(AST.valExpr) == SumExpr: 
                finalVarName = AST.valExpr.outputExpr.name
            elif type(AST.valExpr) == LetExpr:
                expr = AST.valExpr.bodyExpr
                while type(expr) not in [SumExpr]:
                    if type(expr)==LetExpr:
                        expr = expr.bodyExpr
                    else:
                        print("Error: dict type not supported!!")
                        return
                finalVarName = expr.outputExpr.name              
            res = GenerateCPPCode(AST.valExpr, cache) + "const auto& " + AST.varExpr.name + " = " + finalVarName + ";"
        else:
            res = "const auto& " + AST.varExpr.name + " = " + GenerateCPPCode(AST.valExpr, cache) + ";"

        if type(AST.bodyExpr) in [LetExpr, SumExpr]:
            res += GenerateCPPCode(AST.bodyExpr, cache)

        return res

    elif inputType == VarExpr:
        return AST.name

    elif inputType == PairAccessExpr:
        if AST.index == 0:
            return "(" + GenerateCPPCode(AST.pairExpr, cache) + ".first)"
        if AST.index == 1:
            return "(" + GenerateCPPCode(AST.pairExpr, cache) + ".second)"
        else:
            print("Pair Access Error!")

    elif inputType == PromoteExpr:
        res = "("
        if AST.toType == IntType:
            res += "int)("
        elif AST.toType == FloatType:
            res += "double)("
        elif AST.toType == BoolType:
            res += "bool)("
        return res + GenerateCPPCode(AST.bodyExpr, cache) + ")"

    elif inputType == CompareExpr:

        leftRes = GenerateCPPCode(AST.leftExpr, cache)
        rightRes = GenerateCPPCode(AST.rightExpr, cache)

        if (type(AST.leftExpr)==DicLookupExpr or type(AST.rightExpr)==DicLookupExpr):
            if (type(cache[AST.leftExpr.id]) == type(None) or type(cache[AST.rightExpr.id]) == type(None)):
                lookup = leftRes if type(AST.leftExpr)==DicLookupExpr else rightRes
                if AST.compareType == CompareSymbol.EQ:
                    return "(!(" + lookup + "))"
                elif AST.compareType == CompareSymbol.NE:
                    return "(" + lookup + ")" 

        if (type(AST.leftExpr)==ExtFuncExpr and AST.leftExpr.symbol==ExtFuncSymbol.SubStr) or (type(AST.rightExpr)==ExtFuncExpr and AST.rightExpr.symbol==ExtFuncSymbol.SubStr):
            if AST.compareType == CompareSymbol.EQ:
                return "(wcscmp(" + leftRes + ", " + rightRes + ")==0)"
            if AST.compareType == CompareSymbol.EQ:
                return "(wcscmp(" + leftRes + ", " + rightRes + ")!=0)"
                       
            

        if AST.compareType == CompareSymbol.EQ:
            return "(" + leftRes + " == " + rightRes + ")"
        elif AST.compareType == CompareSymbol.NE:
            return "(" + leftRes + " != " + rightRes + ")"            
        elif AST.compareType == CompareSymbol.LT:
            return "(" + leftRes + " < " + rightRes + ")"
        elif AST.compareType == CompareSymbol.LTE:
            return "(" + leftRes + " <= " + rightRes + ")"
        elif AST.compareType == CompareSymbol.GT:
            return "(" + leftRes + " > " + rightRes + ")"
        elif AST.compareType == CompareSymbol.GTE:
            return "(" + leftRes + " >= " + rightRes + ")"

    elif inputType == SumExpr:          
        if type(AST.bodyExpr) == IfExpr: 
            AST.bodyExpr.additionVarExpr = AST.outputExpr
            AST.bodyExpr.isAssignmentSum = AST.isAssignmentSum


        if isInsideBodyOfSum == True:
            if not ((VectorType() == cache[AST.dictExpr.id]) or (type(AST.dictExpr) == DicLookupExpr and DictionaryType() == cache[AST.dictExpr.id])):
                print("\n>>> Attention: Correct code generation for \"Nested Summations\" is not guaranteed in this version!\n")

        res = "\n\n"
              
        if (type(AST.dictExpr) == VarExpr and "db->" in AST.dictExpr.name):
            if globalCoresNo > 1:

                if (AST.dictType).startswith("dense_array"):     
                    cache[AST.id].type_name = "dense_array"
                    isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:] 
                    datasetReaderSumIdxVar = AST.varExpr.name
                    dbSizeVar = newVarName()
                    rangeVar = newVarName()
                    sumVar = newVarName()
                    sumTmpVar = newVarName()
                    innerForVal = AST.varExpr.name
                    threadContainer = newVarName()
                    res += "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                    tmp = GenerateCPPType(cache[AST.id].toType)
                    if "vector" in tmp:
                        tmp = tmp.replace("vector", "tbb::concurrent_vector")
                    outputDictionariesInitializationCode += "vector<" + tmp + "> " + AST.outputExpr.name + "(" + str(int(AST.dictType[12:-1])+1) + ") " + ";"
                    res += "tbb::parallel_for(tbb::blocked_range<size_t>(0, "
                    res += dbSizeVar
                    res += "), [&](const tbb::blocked_range<size_t>& " + rangeVar + ")"
                    res += "{"
                    res += "for (size_t " + innerForVal + "=" + rangeVar + ".begin(), end=" + rangeVar + ".end(); " + innerForVal + "!=end; ++" + innerForVal + "){"
                    
                    isInsideBodyOfSum = True
                    
                    if type(AST.bodyExpr) != IfExpr:
                        res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSum, False, "dense_array")
                    else:
                        AST.bodyExpr.dictType = "dense_array"
                        res += GenerateCPPCode(AST.bodyExpr, cache) + ";"
                    
                    isInsideBodyOfSum = False
                    
                    res += "}});"
                    isInsideBodyOfDatasetReaderSum = None
                    datasetReaderSumIdxVar = None

                # ## Path for generating thread-safe parallel containeeqr 
                # if AST.dictType == "phmap::parallel_flat_hash_map":   
                #     isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:] 
                #     datasetReaderSumIdxVar = AST.varExpr.name
                #     dbSizeVar = newVarName()
                #     rangeVar = newVarName()
                #     sumVar = newVarName()
                #     sumTmpVar = newVarName()
                #     innerForVal = AST.varExpr.name
                #     threadContainer = newVarName()
                #     localStore = VarExpr(newVarName())
                #     counterVar = newVarName()
                #     if GenerateCPPCode(AST.dictExpr, cache) in ["db->lineitem_dataset", "db->orders_dataset", "db->customer_dataset", "db->nation_dataset", "db->region_dataset", "db->supplier_dataset", "db->partsupp_dataset", "db->part_dataset"]:
                #         res += "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                #     else:
                #         res += "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + ".size(); "
                #     outputDictionariesInitializationCode += "\nauto " + AST.outputExpr.name + " = phmap::parallel_flat_hash_map<" + GenerateCPPType(cache[AST.id].fromType) + "," + GenerateCPPType(cache[AST.id].toType)
                #     outputDictionariesInitializationCode += ", phmap::priv::hash_default_hash<" + GenerateCPPType(cache[AST.id].fromType) + ">, phmap::priv::hash_default_eq<" + GenerateCPPType(cache[AST.id].fromType) + ">, std::allocator<std::pair<const " + GenerateCPPType(cache[AST.id].fromType) + "," + GenerateCPPType(cache[AST.id].toType) + ">>, 12, std::mutex>({}); ";
                #     res += "tbb::parallel_for(tbb::blocked_range<size_t>(0, "
                #     res += dbSizeVar
                #     res += "), [&](const tbb::blocked_range<size_t>& " + rangeVar + "){"
                #     res += "for (size_t " + innerForVal + "=" + rangeVar + ".begin(), end=" + rangeVar + ".end(); " + innerForVal + "!=end; ++" + innerForVal + "){"
                #     if type(AST.bodyExpr) != IfExpr:
                #         res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSum, False, AST.dictType)
                #     else:
                #         AST.bodyExpr.dictType = AST.dictType
                #         res += GenerateCPPCode(AST.bodyExpr, cache) + ";"
                #     res += "}" + "});"
                #     isInsideBodyOfDatasetReaderSum = None
                #     datasetReaderSumIdxVar = None

                ## Summation with Vector input and scalar/record output.
                elif type(AST.dictExpr) == VarExpr and (("db->" in AST.dictExpr.name)) and (type(cache[AST.id]) == RecordType or FloatType() == cache[AST.id] or IntType() == cache[AST.id]):
                    
                    isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:]
                    datasetReaderSumIdxVar = AST.varExpr.name
                    dbSizeVar = newVarName()
                    rangeVar = newVarName()
                    sumVar = newVarName()
                    sumTmpVar = newVarName()
                    innerForVal = AST.varExpr.name
                    outputDictionariesInitializationCode += "\nauto " + AST.outputExpr.name + " = " + GenerateCPPZeroOfType(cache[AST.id]) + ";"
                    AST.bodyExpr.additionVarExpr = VarExpr(sumTmpVar)
                    res += "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                    res += AST.outputExpr.name + " = " + "tbb::parallel_reduce(tbb::blocked_range<size_t>(0, "
                    res += dbSizeVar
                    res += "), "
                    res += GenerateCPPZeroOfType(cache[AST.id], AST.dictType)
                    res += ", [&](const tbb::blocked_range<size_t>& " + rangeVar + ", const "
                    res += GenerateCPPType(cache[AST.id]) + "& " + sumVar + ")"
                    res += "{auto " + sumTmpVar + " = " + sumVar + ";"
                    res += "for (size_t " + innerForVal + "=" + rangeVar + ".begin(), end=" + rangeVar + ".end(); " + innerForVal + "!=end; ++" + innerForVal + "){"
                    
                    isInsideBodyOfSum = True

                    if AST.bodyExpr.additionVarExpr is None:
                        res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSumm, False, AST.dictType)
                    else:
                        res += GenerateCPPCode(AST.bodyExpr, cache) + ";"
                    
                    isInsideBodyOfSum = False
                    
                    res += "}return " + sumTmpVar + ";}, plus<" + GenerateCPPType(cache[AST.id]) + ">());"

                    isInsideBodyOfDatasetReaderSum = None
                    datasetReaderSumIdxVar = None
                ## Summation with Vector input and Dictionary output without aggregation.
                elif type(AST.dictExpr) == VarExpr and (("db->" in AST.dictExpr.name)) and DictionaryType() == cache[AST.id] and AST.isAssignmentSum:

                    ## Summation with Vector input and Dictionary output without aggregation when the output has a Vector as Value. 
                    if VectorType() == cache[AST.id].toType:
                        isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:] 
                        datasetReaderSumIdxVar = AST.varExpr.name
                        dbSizeVar = newVarName()
                        rangeVar = newVarName()
                        sumVar = newVarName()
                        sumTmpVar = newVarName()
                        innerForVal = AST.varExpr.name
                        threadContainer = newVarName()
                        localStore = VarExpr(newVarName())
                        counterVar = newVarName()
                        AST.bodyExpr.additionVarExpr = localStore
                        res += "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                        outputDictionariesInitializationCode += "\nauto " + AST.outputExpr.name + " = " + GenerateCPPZeroOfType(cache[AST.id], AST.dictType) + ";"
                        outputDictionariesInitializationCode += "\ntbb::enumerable_thread_specific<" + GenerateCPPType(cache[AST.id]) + "> " + threadContainer + ";"
                        res += "tbb::parallel_for(tbb::blocked_range<size_t>(0, "
                        res += dbSizeVar
                        res += "), [&](const tbb::blocked_range<size_t>& " + rangeVar + "){"
                        res += "auto& " + localStore.name + "=" + threadContainer + ".local();"
                        res += "for (size_t " + innerForVal + "=" + rangeVar + ".begin(), end=" + rangeVar + ".end(); " + innerForVal + "!=end; ++" + innerForVal + "){"
                        
                        isInsideBodyOfSum = True
                        
                        if type(AST.bodyExpr) != IfExpr:
                            res += AdditionCodeGenerator(localStore, AST.bodyExpr, cache, AST.isAssignmentSum, False, AST.dictType)
                        else:
                            res += GenerateCPPCode(AST.bodyExpr, cache) + ";"

                        isInsideBodyOfSum = False

                        res += "}" + "});"
                        res += "for (auto& local : " + threadContainer + ")"
                        res += AST.outputExpr.name + " += local;"
                        isInsideBodyOfDatasetReaderSum = None
                        datasetReaderSumIdxVar = None
                    else:
                        # # =================================================================================
                        # # Base Code
                        # # =================================================================================
                        isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:] 
                        datasetReaderSumIdxVar = AST.varExpr.name
                        dbSizeVar = newVarName()
                        rangeVar = newVarName()
                        sumVar = newVarName()
                        sumTmpVar = newVarName()
                        innerForVal = AST.varExpr.name
                        threadContainer = newVarName()
                        localStore = VarExpr(newVarName())
                        counterVar = newVarName()
                        AST.bodyExpr.additionVarExpr = localStore
                        res += "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                        outputDictionariesInitializationCode += "\nauto " + AST.outputExpr.name + " = " + GenerateCPPZeroOfType(cache[AST.id], AST.dictType) + ";"
                        outputDictionariesInitializationCode += "\ntbb::enumerable_thread_specific<vector<pair<" + GenerateCPPType(cache[AST.id].fromType) + "," + GenerateCPPType(cache[AST.id].toType) + ">>> " + threadContainer + ";"
                        res += "tbb::parallel_for(tbb::blocked_range<size_t>(0, "
                        res += dbSizeVar
                        res += "), [&](const tbb::blocked_range<size_t>& " + rangeVar + ")"
                        res += "{"
                        res += "auto& " + localStore.name + "=" + threadContainer + ".local();"
                        res += "for (size_t " + innerForVal + "=" + rangeVar + ".begin(), end=" + rangeVar + ".end(); " + innerForVal + "!=end; ++" + innerForVal + "){"
                        
                        isInsideBodyOfSum = True
                        
                        if type(AST.bodyExpr) != IfExpr:
                            res += AdditionCodeGenerator(localStore, AST.bodyExpr, cache, AST.isAssignmentSum, False, AST.dictType)
                        else:
                            res += GenerateCPPCode(AST.bodyExpr, cache) + ";"

                        isInsideBodyOfSum = False

                        res += "}});"
                        res += "for (auto& local : " + threadContainer + ")"
                        res += AST.outputExpr.name + ".insert(local.begin(), local.end());"
                        isInsideBodyOfDatasetReaderSum = None
                        datasetReaderSumIdxVar = None
                        # # =================================================================================
                        # # Parallel Single-Phase
                        # # =================================================================================
                        # isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:] 
                        # datasetReaderSumIdxVar = AST.varExpr.name
                        # dbSizeVar = newVarName()
                        # rangeVar = newVarName()
                        # sumVar = newVarName()
                        # sumTmpVar = newVarName()
                        # innerForVal = AST.varExpr.name
                        # threadContainer = newVarName()
                        # counterVar = newVarName()
                        # res += "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                        # outputDictionariesInitializationCode += "\nauto " + AST.outputExpr.name + " = " + GenerateCPPZeroOfType(cache[AST.id], AST.dictType) + ";"
                        # res += "tbb::parallel_for(tbb::blocked_range<size_t>(0, "
                        # res += dbSizeVar
                        # res += "), [&](const tbb::blocked_range<size_t>& " + rangeVar + ")"
                        # res += "{"
                        # res += "for (size_t " + innerForVal + "=" + rangeVar + ".begin(), end=" + rangeVar + ".end(); " + innerForVal + "!=end; ++" + innerForVal + "){"
                        # if type(AST.bodyExpr) != IfExpr:
                        #     res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSum, False, AST.dictType)
                        # else:
                        #     res += GenerateCPPCode(AST.bodyExpr, cache) + ";"
                        # res += "}});"
                        # isInsideBodyOfDatasetReaderSum = None
                        # datasetReaderSumIdxVar = None
                        # # =================================================================================
                        # # Parallel Two-Phase
                        # # =================================================================================


                ## Summation with Vector input and Dictionary output with aggregation.
                elif type(AST.dictExpr) == VarExpr and (("db->" in AST.dictExpr.name)) and DictionaryType() == cache[AST.id] and not AST.isAssignmentSum:
                    # # =================================================================================
                    # # Base Code
                    # # =================================================================================
                    isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:] 
                    datasetReaderSumIdxVar = AST.varExpr.name
                    dbSizeVar = newVarName()
                    rangeVar = newVarName()
                    sumVar = newVarName()
                    sumTmpVar = newVarName()
                    innerForVal = AST.varExpr.name
                    threadContainer = newVarName()
                    localStore = VarExpr(newVarName())
                    counterVar = newVarName()
                    AST.bodyExpr.additionVarExpr = localStore
                    res += "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                    outputDictionariesInitializationCode += "\nauto " + AST.outputExpr.name + " = " + GenerateCPPZeroOfType(cache[AST.id], AST.dictType) + ";"
                    outputDictionariesInitializationCode += "\ntbb::enumerable_thread_specific<" + GenerateCPPType(cache[AST.id]) + "> " + threadContainer + ";"
                    res += "tbb::parallel_for(tbb::blocked_range<size_t>(0, "
                    res += dbSizeVar
                    res += "), [&](const tbb::blocked_range<size_t>& " + rangeVar + "){"
                    res += "auto& " + localStore.name + "=" + threadContainer + ".local();"
                    res += "for (size_t " + innerForVal + "=" + rangeVar + ".begin(), end=" + rangeVar + ".end(); " + innerForVal + "!=end; ++" + innerForVal + "){"
                    
                    isInsideBodyOfSum = True
                    
                    if type(AST.bodyExpr) != IfExpr:
                        res += AdditionCodeGenerator(localStore, AST.bodyExpr, cache, AST.isAssignmentSum, False, AST.dictType)
                    else:
                        res += GenerateCPPCode(AST.bodyExpr, cache) + ";"
                    
                    isInsideBodyOfSum = False
                    
                    res += "}" + "});"
                    res += "for (auto& local : " + threadContainer + ")"
                    # res += AST.outputExpr.name + " += local;"
                    res += "AddMap<" + GenerateCPPType(cache[AST.id]) + "," + GenerateCPPType(cache[AST.id].fromType) + "," + GenerateCPPType(cache[AST.id].toType)+ ">(" + AST.outputExpr.name + ", local);"
                    isInsideBodyOfDatasetReaderSum = None
                    datasetReaderSumIdxVar = None
                    # # =================================================================================
                    # # Parallel Single-Phase
                    # # =================================================================================
                    # isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:] 
                    # datasetReaderSumIdxVar = AST.varExpr.name
                    # dbSizeVar = newVarName()
                    # rangeVar = newVarName()
                    # sumVar = newVarName()
                    # sumTmpVar = newVarName()
                    # innerForVal = AST.varExpr.name
                    # threadContainer = newVarName()
                    # counterVar = newVarName()
                    # res += "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                    # outputDictionariesInitializationCode += "\nauto " + AST.outputExpr.name + " = " + GenerateCPPZeroOfType(cache[AST.id], AST.dictType) + ";"
                    # res += "tbb::parallel_for(tbb::blocked_range<size_t>(0, "
                    # res += dbSizeVar
                    # res += "), [&](const tbb::blocked_range<size_t>& " + rangeVar + "){"
                    # res += "for (size_t " + innerForVal + "=" + rangeVar + ".begin(), end=" + rangeVar + ".end(); " + innerForVal + "!=end; ++" + innerForVal + "){"
                    # if type(AST.bodyExpr) != IfExpr:
                    #     res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSum, False, AST.dictType)
                    # else:
                    #     res += GenerateCPPCode(AST.bodyExpr, cache) + ";"
                    # res += "}" + "});"
                    # isInsideBodyOfDatasetReaderSum = None
                    # datasetReaderSumIdxVar = None
                    # # =================================================================================
                    # # Parallel Two-Phase
                    # # =================================================================================

            elif globalCoresNo == 1:
            
                if (AST.dictType).startswith("dense_array"):
                    cache[AST.id].type_name = "dense_array"
                    isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:] 
                    datasetReaderSumIdxVar = AST.varExpr.name
                    dbSizeVar = newVarName() 
                    res = "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                    if AST.isNestedSumLoop is None:
                        res += "vector<" + GenerateCPPType(cache[AST.id].toType) + "> " + AST.outputExpr.name + "(" + str(int(AST.dictType[12:-1])+1) + ") " + ";" 
                    res += "for (int " + AST.varExpr.name + "=0; " + AST.varExpr.name + "< " + dbSizeVar + "; " + AST.varExpr.name + "++)"
                    res += "{"
                    
                    isInsideBodyOfSum = True
                    
                    if AST.bodyExpr.additionVarExpr is None:
                        res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSum, True, "dense_array")
                    else:
                        AST.bodyExpr.dictType = "dense_array"
                        res += GenerateCPPCode(AST.bodyExpr, cache) + ";"

                    isInsideBodyOfSum = False

                    res += "}"
                    isInsideBodyOfDatasetReaderSum = None
                    datasetReaderSumIdxVar = None                    
                else:
                    isInsideBodyOfDatasetReaderSum = AST.dictExpr.name[4:] 
                    datasetReaderSumIdxVar = AST.varExpr.name
                    dbSizeVar = newVarName() 
                    res = "auto " + dbSizeVar + " = " + GenerateCPPCode(AST.dictExpr, cache) + "_size; "
                    if AST.isNestedSumLoop is None:
                        res += "auto " + AST.outputExpr.name + " = " + GenerateCPPZeroOfType(cache[AST.id], AST.dictType) + ";" 
                    res += "for (int " + AST.varExpr.name + "=0; " + AST.varExpr.name + "< " + dbSizeVar + "; " + AST.varExpr.name + "++)"
                    res += "{"
                    
                    isInsideBodyOfSum = True
                    
                    if AST.bodyExpr.additionVarExpr is None:
                        res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSum)
                    else:
                        res += GenerateCPPCode(AST.bodyExpr, cache) + ";"
                    
                    isInsideBodyOfSum = False
                    
                    res += "}"
                    isInsideBodyOfDatasetReaderSum = None
                    datasetReaderSumIdxVar = None
            else:
                print("Incorrect Cores Number!")    
        else:


            if VectorType() == cache[AST.dictExpr.id]:
                res = "for (auto& " + AST.varExpr.name + " : " + GenerateCPPCode(AST.dictExpr, cache) + ")"
                res += "{"
                res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSum, False)
                res += "}"

            
            elif type(AST.dictExpr) == DicLookupExpr and DictionaryType() == cache[AST.dictExpr.id]:
                res = "for (auto& " + AST.varExpr.name + " : " + GenerateCPPCode(AST.dictExpr, cache) + ")"
                res += "{"
                res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSum, True)
                res += "}"

            else:
                if type(AST.bodyExpr) == IfExpr:
                    AST.bodyExpr.isInNonParallelSum = True

                if AST.isNestedSumLoop is None:
                    outputDictionariesInitializationCode += "\nauto " + AST.outputExpr.name + " = " + GenerateCPPZeroOfType(cache[AST.id], AST.dictType) + ";"

                if type(AST.dictExpr) != VarExpr:
                    res += GenerateCPPCode(AST.dictExpr, cache)

                dictName = None
                expr = AST.dictExpr

                while type(expr) not in [SumExpr, VarExpr, PairAccessExpr, RecAccessExpr]:
                    if type(expr)==LetExpr:
                        expr = expr.bodyExpr
                    else:
                        print("Error: dict type not supported!!!")
                        return
                if type(expr) == SumExpr: dictName = expr.outputExpr.name
                elif type(expr) == VarExpr: dictName = expr.name
                elif type(expr) == RecAccessExpr: 
                    dictName = GenerateCPPCode(AST.dictExpr, cache)
                    res = ""
                elif type(expr) == PairAccessExpr: dictName = GenerateCPPCode(AST.dictExpr, cache)
                
                res += "for (auto& " + AST.varExpr.name + " : " + dictName + ")"
                res += "{"
                if AST.bodyExpr.additionVarExpr is None:
                    res += AdditionCodeGenerator(AST.outputExpr, AST.bodyExpr, cache, AST.isAssignmentSum, True)
                else:
                    res += GenerateCPPCode(AST.bodyExpr, cache) + ";"
                res += "}"

        return res

    elif inputType == DicConsExpr:
        res = GenerateCPPType(cache[AST.id]) + "("
        res += "{"
        for p in AST.initialPairs:
            res += "{"
            res += GenerateCPPCode(p[0], cache)
            res += ", "
            res += GenerateCPPCode(p[1], cache)
            res += "}"
        res += "})"
        return res

    elif inputType == EmptyDicConsExpr:
        if AST.parent == IfExpr and AST.parent.elseBodyExpr == AST:
            return GenerateCPPType(cache[AST.parent.thenBodyExpr.id]) + "({})"
        elif AST.parent == SumExpr and AST.parent.bodyExpr == AST:
            return GenerateCPPType(cache[AST.parent.id]) + "({})"
        else:
            print("Code generation is not supported for this kind of EmptyDicConsExpr: Must have IfExpr/SumExpr parent.")
            return None

    elif inputType == RecConsExpr:
        isInRecConsExpr = True
        res = "make_tuple("
        for p in AST.initialPairs:
            if type(p[1]) == RecAccessExpr and type(p[1].recExpr) == DicLookupExpr:
                p[1].recExpr.isInRecCons = True
            res += GenerateCPPCode(p[1], cache) + ","
        
        isInRecConsExpr = False
        return res[:-1] + ")"

    elif inputType == VecConsExpr:
        res = "vector<"
        for e in AST.exprList:
            if type(e) == RecAccessExpr and type(e.recExpr) == DicLookupExpr:
                e.recExpr.isInRecCons = True
            res += cache[e.id] + ","
            res[:-1] + ">({" + GenerateCPPCode(e, cache) + "});"
        return res

    elif inputType == ConcatExpr:
        return "tuple_cat(" + GenerateCPPCode(AST.rec1, cache) + ",move(" + GenerateCPPCode(AST.rec2, cache) + "))" 

    elif inputType == ExtFuncExpr:
        if AST.symbol == ExtFuncSymbol.StringContains:
            return "(" + GenerateCPPCode(AST.inp3, cache) + ".contains(" + GenerateCPPCode(AST.inp1, cache) + "," + str(len(GenerateCPPCode(AST.inp1, cache))) + "))" #GenerateCPPCode(AST.inp2, cache) + "))"
        if AST.symbol == ExtFuncSymbol.SubStr:
            return GenerateCPPCode(AST.inp1, cache) + ".substr<" + str(1+int(AST.inp3.value)-int(AST.inp2.value)) + ">(" + GenerateCPPCode(AST.inp2, cache) + "," + GenerateCPPCode(AST.inp3, cache) + ")"
        if AST.symbol == ExtFuncSymbol.ToStr:
            return "to_string(" + GenerateCPPCode(AST.inp1, cache) + ")"
        if AST.symbol == ExtFuncSymbol.ExtractYear:
            return "((" + GenerateCPPCode(AST.inp1, cache) + ")/10000)"
        if AST.symbol == ExtFuncSymbol.StartsWith:
            return "(" + GenerateCPPCode(AST.inp1, cache) +".startsWith(" + GenerateCPPCode(AST.inp2, cache) + "))"
        if AST.symbol == ExtFuncSymbol.EndsWith:
            return "(" + GenerateCPPCode(AST.inp1, cache) +".endsWith(" + GenerateCPPCode(AST.inp2, cache) + "))"
        if AST.symbol == ExtFuncSymbol.DictSize:
            return "(" + GenerateCPPCode(AST.inp1, cache) +".size())"
        if AST.symbol == ExtFuncSymbol.FirstIndex:
            return "(" + GenerateCPPCode(AST.inp1, cache) +".firstIndex(" + GenerateCPPCode(AST.inp2, cache) + "))"
        print("Error: Unknown ExtFuncSymbol!")
        return
    else:
        print("Error: Unknown AST: " + str(type(AST)))
        return
######################################################

def GenerateCPPType(typeObj: Type, extraInfo=None):
    if type(typeObj) == BoolType:
        return "bool"
    elif type(typeObj) == IntType:
        return "long"
    elif type(typeObj) == FloatType:
        return "double"
    elif type(typeObj) == StringType:
        if typeObj.charCount==None: 
            return "string"
        elif typeObj.charCount:
            return "VarChar<" + str(typeObj.charCount) + ">"
        # else:
        #     return "char"
    elif type(typeObj) == DictionaryType:
        if (extraInfo is not None):
            res = extraInfo + "<"
        else:
            res = "phmap::flat_hash_map<"

        res += GenerateCPPType(typeObj.fromType)
        res += ","
        res += GenerateCPPType(typeObj.toType)
        res += ">"
        return res
    elif type(typeObj) == RecordType:
        res = "tuple<"
        for p in typeObj.typesList:
            res += GenerateCPPType(p[1]) + ","
        res = res[:-1] + ">"
        return res
    elif type(typeObj) == VectorType:
        res = "vector<"
        for e in typeObj.exprTypes:
            res += GenerateCPPType(e) + ","
            res = res[:-1] + ">"
        return res
    else:
        return "Error: Type not supported: " + str(type(typeObj))

######################################################

def GenerateCPPZeroOfType(typeObj, extraInfo=None):
    if type(typeObj) == BoolType:
        return "false"
    elif type(typeObj) == IntType:
        return "0"
    elif type(typeObj) == FloatType:
        return "0.0"
    elif type(typeObj) == StringType:
        if typeObj.charCount==None or typeObj.charCount > 1: 
            return ""
        else:
            return ''
    elif type(typeObj) == DictionaryType:
        return GenerateCPPType(typeObj, extraInfo) + "({})"
    elif type(typeObj) == RecordType:
        res = "make_tuple("
        for p in typeObj.typesList:
            res += GenerateCPPZeroOfType(p[1]) + ","
        res = res[:-1] + ")"
        return res
    elif type(typeObj) == VectorType:
        res = "vector<"
        for t in typeObj.exprTypes:
            res += GenerateCPPZeroOfType(t) + ","
        res = res[:-1] + ">({})"
        return res
    else:
        return "Error: Type not supported: " + type(typeObj)


def AdditionCodeGenerator(lhsVarExpr, rhsExpr, cache, isAssignmentSum=False, isInNonParallelSum=False, dictType=None):
    global globalCoresNo
    global isInRecConsExpr

    res = ""
    rhsKey = ""
    rhsVal = ""
    if cache[rhsExpr.id] != DictionaryType():
        if isAssignmentSum:
            res += lhsVarExpr.name + " = " + GenerateCPPCode(rhsExpr, cache) + ";" 
        else:
            if type(rhsExpr) == RecConsExpr:
                isInRecConsExpr = True
                for i in range(0, len(rhsExpr.exprList)):
                    res += "get<" + str(i) + ">(" + lhsVarExpr.name + ") += " + GenerateCPPCode(rhsExpr.exprList[i], cache) +";\n"
                isInRecConsExpr = False
            else:
                res += lhsVarExpr.name + " += " + GenerateCPPCode(rhsExpr, cache) + ";" 

    else:
        if type(rhsExpr) == DicConsExpr:
            rhsKey = GenerateCPPCode(rhsExpr.exprList[0], cache)
            if VectorType() == cache[rhsExpr.exprList[1].id]:
                rhsVal = GenerateCPPCode(rhsExpr.exprList[1].exprList[0], cache)
                res+= lhsVarExpr.name + "[" + rhsKey + "].emplace_back(" + rhsVal + ");"
                return res
            elif dictType == "dense_array":
                rhsVal = GenerateCPPCode(rhsExpr.exprList[1], cache)
                res+= lhsVarExpr.name + "[" + rhsKey + "] = " + rhsVal + ";"
                return res
            else:
                rhsVal = GenerateCPPCode(rhsExpr.exprList[1], cache)
        elif type(rhsExpr) == VarExpr:
            rhsKey = "(" + rhsExpr.name + ".begin())->first" 
            rhsVal = "(" + rhsExpr.name + ".begin())->second"
        elif type(rhsExpr) == LetExpr:
            tmpName = newVarName()
            res += "const auto& " + tmpName + " = " + GenerateCPPCode(rhsExpr, cache) + ";"
            rhsKey = "(" + tmpName + ".begin())->first" 
            rhsVal = "(" + tmpName + ".begin())->second"
        elif type(rhsExpr) == IfExpr:
            tmpName = newVarName()
            res += "const auto& " + tmpName + " = " + GenerateCPPCode(rhsExpr, cache) + ";"
            rhsKey = "(" + tmpName + ".begin())->first" 
            rhsVal = "(" + tmpName + ".begin())->second"
        elif type(rhsExpr) == SumExpr:
            rhsExpr.outputExpr = lhsVarExpr
            rhsExpr.isNestedSumLoop = True
            return GenerateCPPCode(rhsExpr, cache)+";"
        else:
            return "Error: rhsExpr is not supported: " + type(rhsExpr)

        if isAssignmentSum:
            if isInNonParallelSum == False and dictType != "phmap::parallel_flat_hash_map":
                if globalCoresNo==1:
                    res += lhsVarExpr.name + ".emplace("
                else:
                    res += lhsVarExpr.name + ".emplace_back("
                res += rhsKey
                res += ", "
                res += rhsVal
                res += ");"
            else:
                res += lhsVarExpr.name + "["
                res += rhsKey
                res += "] = "
                res += rhsVal
                res += ";"                
        else:
            res += lhsVarExpr.name + "["
            res += rhsKey
            res += "] += "
            res += rhsVal
            res += ";"


        # res += lhsVarExpr.name + "["
        # res += rhsKey
        # res += "] += "
        # res += rhsVal
        # res += ";"

            
    return res


def GenerateCPPQueryFile(queryNo, queryCode, path="SDQLCPP/SDQLCPP/Queries/"):
    global outputDictionariesInitializationCode

    res = ""
    preCode = "#pragma once\n#include \"../Headers/QIncludes.h\"\nauto RunQ" + str(queryNo) + "(TPCHDataLoaderColumn& db, Timer& t)\n{\n\tint qNo = " + str(queryNo) + ";\n\tt.Reset();\n\t"
    preCode += outputDictionariesInitializationCode
    preCode += "\n\n\tconst auto& out ="
    #postCode = "\n\treturn out;\n}"
    postCode = "\n\tt.StoreElapsedTime(qNo);\n\treturn out;\n}"
    res += preCode
    res += "\n\t"
    res += queryCode
    res += "\n"
    res += postCode

    outputDictionariesInitializationCode = ""

    with open(os.path.join(os.path.dirname(__file__), 'SDQLCPP/SDQLCPP/Queries/') + "Q" + str(queryNo) + "_Parallel.h", "w") as myfile:
        myfile.write(res)
        myfile.close()

    print(">>> Q" + str(queryNo) + ": C++ code generated.")


def GenerateCPPFileFromIR(fileName, code, path=""):
    res = ""
    preCode = "#pragma once\n#include \"../Headers/QIncludes.h\"\nauto Run(TPCHDataLoaderColumn& db)\n{\n\t\n\n\tconst auto& out ="
    postCode = "\n\treturn out;\n}"
    res += preCode
    res += "\n\t"
    res += code
    res += "\n"
    res += postCode

    with open(os.path.join(os.path.dirname(__file__), path) + "/" + fileName + ".h", "w") as myfile:
        myfile.write(res)
        myfile.close()

    print(">>> " + str(fileName) + ": C++ code generated.")

# This function is only for use in new python frontend compiler
def FinalizeCPPCodeNoFile(code, out_type, db_code, is_columnar_input, is_fast_dict_input, abbrOutType, filename):
    global outputDictionariesInitializationCode

    res = ""
    preCode = "\t"
    postCode = ""

    preCode += """

    PyObject * db_;
    if (!PyArg_ParseTuple(args, "O", &db_)){
        PyErr_SetString(PyExc_ValueError,"Error while parsing the sum inputs.");
        return NULL;
    }
    
    """

    if is_columnar_input:
        preCode +="""
    DB dbobj;
    DB* db = &dbobj;
    
"""

    preCode += db_code + "\n\n\t" + outputDictionariesInitializationCode
    preCode += "\t"

    if type(out_type) != DictionaryType:
        if out_type == int:
            postCode += "\n\treturn PyLong_FromLong((long)out);\n"
        elif out_type == float:
            postCode += "\n\treturn PyFloat_FromDouble(out);\n"
    elif type(out_type) == DictionaryType:
        postCode += """
    """ + abbrOutType + """* result = (""" + abbrOutType + """*)PyObject_CallObject(PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString(\"""" + filename + """_fastdict_compiled\")), (char*)"new_""" + abbrOutType + """\"), nullptr), nullptr);
    *(result->dict) = out;

    return (PyObject*)PyObject_CallObject(PyObject_GetAttrString(PyImport_Import(PyUnicode_FromString("sdqlpy.fastd")), "fastd"), Py_BuildValue("(OO)", result, PyUnicode_FromString(\"""" + filename + """\")));
"""

    else:
        "Error: out_type is not defined!"

    res += preCode
    res += "\n\t"
    res += code
    res += "\n"
    res += postCode

    outputDictionariesInitializationCode = ""
    print(">>> C++ code generated.")
    return res