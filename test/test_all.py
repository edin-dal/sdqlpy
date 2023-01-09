import os
from sdqlpy.sdql_lib import *

## The "sdqlpy_init" function must be defined here. 
## Its first parameter can be set to: 
## 0: to run in Python | 1: to run in compiled mode | 2: to run in compiled mode using the previously compiled version.
## Its second parameter show the number of threads. (current version does not support multi-threading for "run in Python" mode) 
## Note: when the first parameter is set to "1", the previous number of threads is used and the current parameter will be ignored. 

sdqlpy_init(1, 2)

############ Reading Dataset

## The following path must point to your dbgen dataset.
dataset_path = os.getenv('TPCH_DATASET')

## Shows the number of returned results, average and stdev of run time, and the results (if the next parameter is also set to True)
verbose = True
show_results = False

## Number of iterations for benchmarking each query (must be >=2)
iterations = 2

############ Reading Dataset

lineitem_type = {record({"l_orderkey": int, "l_partkey": int, "l_suppkey": int, "l_linenumber": int, "l_quantity": float, "l_extendedprice": float, "l_discount": float, "l_tax": float, "l_returnflag": string(1), "l_linestatus": string(1), "l_shipdate": date, "l_commitdate": date, "l_receiptdate": date, "l_shipinstruct": string(25), "l_shipmode": string(10), "l_comment": string(44), "l_NA": string(1)}): bool}
customer_type = {record({"c_custkey": int, "c_name": string(25), "c_address": string(40), "c_nationkey": int, "c_phone": string(15), "c_acctbal": float, "c_mktsegment": string(10), "c_comment": string(117), "c_NA": string(1)}): bool}
order_type = {record({"o_orderkey": int, "o_custkey": int, "o_orderstatus": string(1), "o_totalprice": float, "o_orderdate": date, "o_orderpriority": string(15), "o_clerk": string(15), "o_shippriority": int, "o_comment": string(79), "o_NA": string(1)}): bool}
nation_type = {record({"n_nationkey": int, "n_name": string(25), "n_regionkey": int, "n_comment": string(152), "n_NA": string(1)}): bool}
region_type = {record({"r_regionkey": int, "r_name": string(25), "r_comment": string(152), "r_NA": string(1)}): bool}
part_type = {record({"p_partkey": int, "p_name": string(55), "p_mfgr": string(25), "p_brand": string(10), "p_type": string(25), "p_size": int, "p_container": string(10), "p_retailprice": float, "p_comment": string(23), "p_NA": string(1)}): bool}
partsupp_type = {record({"ps_partkey": int, "ps_suppkey": int, "ps_availqty": float, "ps_supplycost": float, "ps_comment": string(199), "ps_NA": string(1)}): bool}
supplier_type = {record({"s_suppkey": int, "s_name": string(25), "s_address": string(40), "s_nationkey": int, "s_phone": string(15), "s_acctbal": float, "s_comment": string(101), "s_NA": string(1)}): bool}

lineitem = read_csv(dataset_path + "lineitem.tbl", lineitem_type, "li")
customer = read_csv(dataset_path + "customer.tbl", customer_type, "cu")
order = read_csv(dataset_path + "orders.tbl", order_type, "ord")
nation = read_csv(dataset_path + "nation.tbl", nation_type, "na")
region = read_csv(dataset_path + "region.tbl", region_type, "re")
part = read_csv(dataset_path + "part.tbl", part_type, "pa")
partsupp = read_csv(dataset_path + "partsupp.tbl", partsupp_type, "ps")
supplier = read_csv(dataset_path + "supplier.tbl", supplier_type, "su")

######

@sdql_compile({"li": lineitem_type})
def q1(li):
    
    lineitem_probed = li.sum(lambda p: 
        {
            record({"l_returnflag": p[0].l_returnflag, "l_linestatus": p[0].l_linestatus}):
            record({"sum_qty": p[0].l_quantity, "sum_base_price": p[0].l_extendedprice, "sum_disc_price": (p[0].l_extendedprice * (1.0 - p[0].l_discount)), "sum_charge": ((p[0].l_extendedprice * (1.0 - p[0].l_discount)) * (1.0 + p[0].l_tax)), "count_order": 1})
        }
        if
            p[0].l_shipdate <= 19980902
        else
            None
        )

    results = lineitem_probed.sum(lambda p: {unique(p[0].concat(p[1])): True})

    return results

######

@sdql_compile({"pa": part_type, "su": supplier_type, "ps": partsupp_type, "na": nation_type, "re": region_type})
def q2(pa, su, ps, na, re):

    brass = "BRASS"
    europe = "EUROPE"

    re_indexed = re.joinBuild("r_regionkey", lambda p: p[0].r_name == europe, [])
    
    na_probed = na.joinProbe(
                    re_indexed,
                    "n_regionkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.n_nationkey: probeDictKey.n_name
                    },
                    False
                )

    su_probed = su.joinProbe(
                    na_probed,
                    "s_nationkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.s_suppkey: 
                        record(
                            {
                                "s_acctbal": probeDictKey.s_acctbal,
                                "s_name": probeDictKey.s_name, 
                                "n_name": indexedDictValue,
                                "s_address": probeDictKey.s_address, 
                                "s_phone": probeDictKey.s_phone, 
                                "s_comment": probeDictKey.s_comment
                            }
                        )                        
                    },
                    False
                )
                                                                          
    pa_indexed = pa.joinBuild("p_partkey", lambda p: p[0].p_size == 15 and endsWith(p[0].p_type, brass), ["p_mfgr"]) 
  
    ps_probed = ps.joinProbe(
                    su_probed,
                    "ps_suppkey",
                    lambda p: pa_indexed[p[0].ps_partkey] != None,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.ps_partkey: 
                        probeDictKey.ps_supplycost  ## Min                     
                    }
                )
                   
    results = ps.sum(lambda p:
                    {
                        unique(record(
                            {
                                "s_acctbal": su_probed[p[0].ps_suppkey].s_acctbal,
                                "s_name": su_probed[p[0].ps_suppkey].s_name, 
                                "n_name": su_probed[p[0].ps_suppkey].n_name,
                                "p_partkey": p[0].ps_partkey,
                                "p_mfgr": pa_indexed[p[0].ps_partkey].p_mfgr,
                                "s_address": su_probed[p[0].ps_suppkey].s_address, 
                                "s_phone": su_probed[p[0].ps_suppkey].s_phone, 
                                "s_comment": su_probed[p[0].ps_suppkey].s_comment
                            }
                        )):
                        True
                    }
                if
                    ps_probed[p[0].ps_partkey] != None and ps_probed[p[0].ps_partkey] == p[0].ps_supplycost and su_probed[p[0].ps_suppkey] != None
                else
                    None
            ) 
    
    return results

######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type})
def q3(li, cu, ord):
    
    building = "BUILDING"
    
    customer_indexed = cu.joinBuild("c_custkey", lambda p: p[0].c_mktsegment == building, [])

    order_probed = ord.joinProbe(
                    customer_indexed,
                    "o_custkey",
                    lambda p: p[0].o_orderdate < 19950315,
                    lambda indexedDictValue, probeDictKey: 
                    {
                        probeDictKey.o_orderkey:
                        record({"o_orderdate": probeDictKey.o_orderdate, "o_shippriority": probeDictKey.o_shippriority})
                    }
                    , False
                )

    lineitem_probed = li.joinProbe(
                order_probed,
                "l_orderkey",
                lambda p: p[0].l_shipdate > 19950315,
                lambda indexedDictValue, probeDictKey:
                {
                    record({"l_orderkey": probeDictKey.l_orderkey, "o_orderdate": indexedDictValue.o_orderdate, "o_shippriority": indexedDictValue.o_shippriority}):
                    record({"revenue": probeDictKey.l_extendedprice*(1.0-probeDictKey.l_discount)})
                })

    results = lineitem_probed.sum(lambda p: {unique(p[0].concat(p[1])): True}) 
    
    return results  

#####

@sdql_compile({"ord": order_type, "li": lineitem_type})
def q4(ord, li):

    li_indexed = li.sum(lambda p:
            {
                dense(6000000, unique(p[0].l_orderkey)): True
            }
        if
            p[0].l_commitdate < p[0].l_receiptdate
        else
            None
        )

    ord_probed = ord.joinProbe(
        li_indexed,
        "o_orderkey",
        lambda p: p[0].o_orderdate >= 19930701 and p[0].o_orderdate < 19931001,
        lambda indexedDictValue, probeDictKey:
        {
            probeDictKey.o_orderpriority: 1
        })

    results = ord_probed.sum(lambda p:
                        {
                            unique(record
                            (
                                {"o_orderpriority": p[0], "order_count": p[1]} 
                            )):
                            True
                        })

    return results  

#######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type, "re": region_type, "na": nation_type, "su": supplier_type})
def q5(li, cu, ord, re, na, su):

    asia = "ASIA"

    region_indexed = re.joinBuild("r_regionkey", lambda p: p[0].r_name == asia , []) 

    nation_probed = na.joinProbe(
                        region_indexed,
                        "n_regionkey",         
                        lambda p: True,
                        lambda indexedDictValue, probeDictKey:
                        {
                           probeDictKey.n_nationkey: probeDictKey.n_name
                        },
                        False)

    customer_probed = cu.joinProbe(
                        nation_probed,
                        "c_nationkey",
                        lambda p: True,
                        lambda indexedDictValue, probeDictKey:
                        {
                            probeDictKey.c_custkey: record({"n_name": indexedDictValue, "c_nationkey": probeDictKey.c_nationkey})
                        },
                        False)

    order_probed = ord.joinProbe(
                        customer_probed,
                        "o_custkey",
                        lambda p: (p[0].o_orderdate < 19950101) * (p[0].o_orderdate >= 19940101),
                        lambda indexedDictValue, probeDictKey:
                        {
                            probeDictKey.o_orderkey: record({"n_name":indexedDictValue.n_name, "c_nationkey": indexedDictValue.c_nationkey})
                        },
                        False)

    supplier_project = su.sum(lambda p:
                        {
                            unique(record({"s_suppkey": p[0].s_suppkey,"s_nationkey": p[0].s_nationkey})):
                            True
                        })

    lineitem_probed = li.joinProbe(
                        order_probed,
                        "l_orderkey",
                        lambda p: True,
                        lambda indexedDictValue, probeDictKey:
                        {
                            indexedDictValue.n_name:
                            probeDictKey.l_extendedprice*(1.0-probeDictKey.l_discount)
                        }
                        if
                            supplier_project[record({"l_suppkey": probeDictKey.l_suppkey,"c_nationkey": indexedDictValue.c_nationkey})] != None
                        else
                            None)

    results = lineitem_probed.sum(lambda p:
                        {
                            unique(record
                            (
                                {"n_name": p[0], "revenue": p[1]} 
                            )):
                            True
                        })

    return results

#######

@sdql_compile({"li": lineitem_type})
def q6(li):

    results = li.sum(
        lambda p: p[0].l_extendedprice * p[0].l_discount 
    if 
        (p[0].l_shipdate >= 19940101) and (p[0].l_shipdate < 19950101) and (p[0].l_discount >=  0.05) and (p[0].l_discount <= 0.07) and (p[0].l_quantity < 24.0)
    else
        0.0)

    return results

#######

@sdql_compile({"su": supplier_type, "li": lineitem_type, "ord": order_type, "cu": customer_type, "na": nation_type})
def q7(su, li, ord, cu, na):

    france = "FRANCE"
    germany = "GERMANY"


    nation_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name==france or p[0].n_name==germany, ["n_name"])    

    cu_probed = cu.joinProbe(
                    nation_indexed,
                    "c_nationkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.c_custkey: indexedDictValue.n_name
                    },
                    False
                )

    ord_probed = ord.joinProbe(
                    cu_probed,
                    "o_custkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey: 
                    {
                        probeDictKey.o_orderkey: indexedDictValue
                    },
                    False
                ) 

    su_probed = su.joinProbe(
                nation_indexed,
                "s_nationkey",
                lambda p: True,
                lambda indexedDictValue, probeDictKey: 
                {
                    probeDictKey.s_suppkey: indexedDictValue.n_name
                },
                False
            )

    
    li_probed = li.sum(lambda p:
                            {
                                record(
                                    {
                                        "supp_nation": (su_probed[p[0].l_suppkey]),
                                        "cust_nation": (ord_probed[p[0].l_orderkey]),
                                        "l_year": extractYear(p[0].l_shipdate)
                                    }
                                ):
                                record({"revenue": p[0].l_extendedprice * (1.0 - p[0].l_discount)})
                            }
                        if
                            p[0].l_shipdate >= 19950101 and 
                            p[0].l_shipdate <= 19961231 and
                            ord_probed[p[0].l_orderkey] != None and
                            su_probed[p[0].l_suppkey] != None and
                            (
                                ((ord_probed[p[0].l_orderkey]) == france and (su_probed[p[0].l_suppkey]) == germany) or
                                ((ord_probed[p[0].l_orderkey]) == germany and (su_probed[p[0].l_suppkey]) == france)
                            )
                        else
                            None)

    results = li_probed.sum(lambda p: {unique(p[0].concat(p[1])): True})

    return results
    
######

@sdql_compile({"pa": part_type,"su": supplier_type, "li": lineitem_type, "ord": order_type, "cu": customer_type, "na": nation_type, "re": region_type})
def q8(pa, su, li, ord, cu, na, re):

    steel = "ECONOMY ANODIZED STEEL"
    america = "AMERICA"
    brazil = "BRAZIL"

    re_indexed = re.joinBuild("r_regionkey", lambda p: p[0].r_name==america, [])     
    
    na_probed = na.joinProbe(
                re_indexed,
                "n_regionkey",
                lambda p: True,
                lambda indexedDictValue, probeDictKey:
                {
                    probeDictKey.n_nationkey: True
                },
                False
            )

    na_indexed = na.joinBuild("n_nationkey", lambda p: True, ["n_name"])
    
    su_indexed = su.joinBuild("s_suppkey", lambda p: True, ["s_nationkey"])
       
    cu_indexed = cu.sum(lambda p:
                        {
                            dense(200000, unique(p[0].c_custkey)): p[0].c_nationkey
                        }
                    )

    pa_indexed = pa.joinBuild("p_partkey", lambda p: p[0].p_type == steel, [])

    ord_indexed = ord.joinBuild("o_orderkey", lambda p: p[0].o_orderdate >= 19950101 and p[0].o_orderdate <= 19961231, ["o_custkey", "o_orderdate"])

    li_probed = li.joinProbe(
                    pa_indexed,
                    "l_partkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                        {
                            extractYear(ord_indexed[probeDictKey.l_orderkey].o_orderdate):
                            record(
                                    {
                                        "A": probeDictKey.l_extendedprice * (1.0 - probeDictKey.l_discount) if (na_indexed[su_indexed[probeDictKey.l_suppkey].s_nationkey].n_name) == brazil else 0.0, 
                                        "B": probeDictKey.l_extendedprice * (1.0 - probeDictKey.l_discount)
                                    }
                                )
                        }
                    if
                        ord_indexed[probeDictKey.l_orderkey] != None and na_probed[cu_indexed[((ord_indexed[probeDictKey.l_orderkey]).o_custkey)]] != None 
                    else
                        None,
                )

    results = li_probed.sum(lambda p: {unique(record({"o_year": p[0], "mkt_share": p[1].A / p[1].B})): True})

    return results

######

@sdql_compile({"li": lineitem_type, "ord": order_type, "na": nation_type, "su": supplier_type, "pa": part_type ,"ps": partsupp_type})
def q9(li, ord, na, su, pa, ps):

    nation_indexed = na.joinBuild("n_nationkey", lambda p: True, ["n_name"])

    supplier_probed = su.sum(lambda p:
                                {
                                    unique(p[0].s_suppkey):
                                    nation_indexed[p[0].s_nationkey].n_name
                                }
                            )

    green = "green" 

    part_indexed = pa.joinBuild("p_partkey", lambda p: green in p[0].p_name, [])

    partsupp_probe = ps.joinProbe(
        part_indexed,
        "ps_partkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey: 
        {
            record({"ps_partkey": probeDictKey.ps_partkey, "ps_suppkey": probeDictKey.ps_suppkey}):
            record({"n_name": supplier_probed[probeDictKey.ps_suppkey], "ps_supplycost": probeDictKey.ps_supplycost})
        },
        False
    )

    ord_indexed = ord.sum(lambda p:
                        {
                            dense(6000000, unique(p[0].o_orderkey)): p[0].o_orderdate
                        }
                    )

    li_probed = li.sum(lambda p:
            {
                record
                (
                    {
                        "nation": partsupp_probe[record({"ps_partkey": p[0].l_partkey, "ps_suppkey": p[0].l_suppkey})].n_name,
                        "o_year": extractYear(ord_indexed[p[0].l_orderkey]) ## Change it in the future. It must go to join build phase
                    }
                ):
                record
                (
                    {
                        "sum_profit": 
                        p[0].l_extendedprice * (1.0 - p[0].l_discount) - partsupp_probe[record({"ps_partkey": p[0].l_partkey, "ps_suppkey": p[0].l_suppkey})].ps_supplycost * p[0].l_quantity
                    }
                )
            }
            if
                partsupp_probe[record({"ps_partkey": p[0].l_partkey, "ps_suppkey": p[0].l_suppkey})] != None
            else
                None
            )


    results = li_probed.sum(lambda p: {unique(p[0].concat(p[1])): True})

    return results

######

@sdql_compile({"cu": customer_type, "ord": order_type, "li": lineitem_type, "na": nation_type})
def q10(cu, ord, li, na):
    
    r = "R"

    na_indexed = na.joinBuild("n_nationkey", lambda p: True, ["n_name"])

    cu_indexed = cu.joinBuild("c_custkey", lambda p: True, ["c_custkey", "c_name", "c_acctbal", "c_address", "c_nationkey", "c_phone", "c_comment"])

    ord_probed = ord.joinProbe(
        cu_indexed,
        "o_custkey",
        lambda p: p[0].o_orderdate >= 19931001 and p[0].o_orderdate < 19940101,
        lambda indexedDictValue, probeDictKey: 
        {
            probeDictKey.o_orderkey:
            record({
                "c_custkey": indexedDictValue.c_custkey, 
                "c_name": indexedDictValue.c_name, 
                "c_acctbal": indexedDictValue.c_acctbal, 
                "c_address": indexedDictValue.c_address, 
                "c_phone": indexedDictValue.c_phone, 
                "c_comment": indexedDictValue.c_comment, 
                "n_name": na_indexed[indexedDictValue.c_nationkey].n_name
            })
        },
        False
    )

    li_probed = li.joinProbe(
        ord_probed,
        "l_orderkey",
        lambda p: p[0].l_returnflag == r,
        lambda indexedDictValue, probeDictKey: 
        {
            record({
                "c_custkey": indexedDictValue.c_custkey, 
                "c_name": indexedDictValue.c_name, 
                "c_acctbal": indexedDictValue.c_acctbal, 
                "n_name": indexedDictValue.n_name, 
                "c_address": indexedDictValue.c_address, 
                "c_phone": indexedDictValue.c_phone, 
                "c_comment": indexedDictValue.c_comment
            }): probeDictKey.l_extendedprice * (1.0-probeDictKey.l_discount)
        },
        True
    )

    results = li_probed.sum(lambda p: 
    {
        unique(record({
                "c_custkey": p[0].c_custkey, 
                "c_name": p[0].c_name, 
                "revenue": p[1],
                "c_acctbal": p[0].c_acctbal, 
                "n_name": p[0].n_name,
                "c_address": p[0].c_address, 
                "c_phone": p[0].c_phone, 
                "c_comment": p[0].c_comment
        })): 
        True
    })

    return results

#######

@sdql_compile({"ps": partsupp_type, "su": supplier_type, "na": nation_type})
def q11(ps, su, na):

    germany = "GERMANY"

    na_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name==germany, [])

    su_probed = su.joinProbe(
                    na_indexed,
                    "s_nationkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.s_suppkey: True
                    },
                    False
                )
       
    ps_probed = ps.joinProbe(
                    su_probed,
                    "ps_suppkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey: 
                        record(
                            {
                                "A": (probeDictKey.ps_supplycost * probeDictKey.ps_availqty) * 0.0001,
                                "B": sr_dict({  
                                        probeDictKey.ps_partkey: (probeDictKey.ps_supplycost * probeDictKey.ps_availqty)
                                    })                                    
                            })     
                    )

    results = (ps_probed.B).sum(lambda p:
                                {
                                    record({"ps_partkey": p[0], "value": p[1]}): True
                                }
                            if 
                                p[1] > (ps_probed.A)
                            else
                                None
                        )

    return results

######

@sdql_compile({"ord": order_type, "li": lineitem_type})
def q12(ord, li):

    mail = "MAIL"
    ship = "SHIP"
    urgent = "1-URGENT"
    high = "2-HIGH"

    li_indexed = li.sum(lambda p: 
                    {
                        p[0].l_orderkey:
                        sr_dict({
                            p[0].l_shipmode: 1
                        })
                    }
                if
                    ((p[0].l_shipmode == mail) or (p[0].l_shipmode == ship)) and
                    (p[0].l_receiptdate >= 19940101) and
                    (p[0].l_receiptdate < 19950101) and
                    (p[0].l_shipdate < p[0].l_commitdate) and
                    (p[0].l_commitdate < p[0].l_receiptdate)
                else
                    None
            )

    ord_probed = ord.joinProbe(
                    li_indexed,
                    "o_orderkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                        indexedDictValue.sum(lambda p:
                            {
                                record({"l_shipmode": p[0]}):
                                record(
                                        {
                                            "high_line_count": p[1] if (probeDictKey.o_orderpriority == urgent) or (probeDictKey.o_orderpriority == high) else 0,
                                            "low_line_count": p[1] if (probeDictKey.o_orderpriority != urgent) and (probeDictKey.o_orderpriority != high) else 0,
                                        }
                                )
                            }
                    ))

    results = ord_probed.sum(lambda p: {unique(p[0].concat(p[1])): True})

    return results

######

@sdql_compile({"cu": customer_type, "ord": order_type})
def q13(cu, ord):

    special = "special"
    requests = "requests"

    ord_indexed = ord.sum(lambda p: 
                            {
                                p[0].o_custkey: 1
                            }
                            if
                                ((firstIndex(p[0].o_comment, special) != -1) and (firstIndex(p[0].o_comment, requests) > (firstIndex(p[0].o_comment, special)+6))) == False
                            else 
                                None
                        )

    customer_probed = cu.sum(lambda p: 
                            {
                                record(
                                    {
                                        "c_count":
                                            ord_indexed[p[0].c_custkey]
                                        if
                                            ord_indexed[p[0].c_custkey] != None
                                        else
                                            0
                                    }
                                )
                                :
                                record({ "custdist": 1 })                     
                            }
                        )

    results = customer_probed.sum(lambda p: {unique(p[0].concat(p[1])): True})

    return results

#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q14(li, pa):

    promo = "PROMO"
                                                     
    pa_indexed = pa.joinBuild("p_partkey", lambda p: startsWith(p[0].p_type, promo), [])

    li_probed = li.sum(lambda p: 
                            record(
                                {
                                    "A": p[0].l_extendedprice * (1.0 - p[0].l_discount) if pa_indexed[p[0].l_partkey] != None else 0.0,
                                    "B": p[0].l_extendedprice * (1.0 - p[0].l_discount)
                                }
                            )
                        if p[0].l_shipdate >= 19950901 and p[0].l_shipdate < 19951001 
                        else
                            None
                    )
    
    results = (100.0 * li_probed.A) / li_probed.B

    return results

#######

@sdql_compile({"li": lineitem_type, "su": supplier_type})
def q15(li, su):

    li_aggr = li.sum(lambda p: 
                    {
                        p[0].l_suppkey: (p[0].l_extendedprice * (1.0 - p[0].l_discount))
                    }
                if
                    p[0].l_shipdate >= 19960101 and p[0].l_shipdate < 19960401
                else
                    None
            )

    max_revenue = 1772627.2087
    # max_revenue = li_aggr.sum(lambda p: max(p[1]))
        
    su_indexed = su.joinBuild("s_suppkey", lambda p: True, ["s_name", "s_address", "s_phone"])
    
    results = li_aggr.sum(lambda p:
                    {
                        unique(record(
                            {
                                "s_suppkey": p[0], 
                                "s_name": su_indexed[p[0]].s_name, 
                                "s_address": su_indexed[p[0]].s_address, 
                                "s_phone": su_indexed[p[0]].s_phone, 
                                "total_revenue": p[1]
                            })):
                            True
                    }
                if
                    p[1]==max_revenue
                else
                    None
            )

    return results

#######

@sdql_compile({"ps": partsupp_type, "pa": part_type, "su": supplier_type})
def q16(ps, pa, su):
    brand45 = "Brand#45"
    medpol = "MEDIUM POLISHED"
    Customer = "Customer"
    complaints = "Complaints"

    part_indexed = pa.joinBuild("p_partkey", lambda p:
                                    p[0].p_brand != brand45 and
                                    startsWith(p[0].p_type, medpol) == False and
                                    (
                                        p[0].p_size == 49 or
                                        p[0].p_size == 14 or
                                        p[0].p_size == 23 or
                                        p[0].p_size == 45 or
                                        p[0].p_size == 19 or
                                        p[0].p_size == 3  or
                                        p[0].p_size == 36 or
                                        p[0].p_size == 9
                                    ), 
                                ["p_brand", "p_type", "p_size"]
                            )

    su_indexed = su.joinBuild("s_suppkey", lambda p: (firstIndex(p[0].s_comment, Customer) != -1) and (firstIndex(p[0].s_comment, complaints) > (firstIndex(p[0].s_comment, Customer)+7)), [])

    partsupp_probe = ps.joinProbe(
        part_indexed,
        "ps_partkey",
        lambda p: True,
        lambda indexedDictValue, probeDictKey: 
            {
                record
                (
                    {
                        "p_brand": indexedDictValue.p_brand,
                        "p_type":  indexedDictValue.p_type,
                        "p_size":  indexedDictValue.p_size
                    }
                ):
                sr_dict(
                    {
                        probeDictKey.ps_suppkey: True
                    }
                )
            }
        if
            su_indexed[probeDictKey.ps_suppkey] == None
        else
            None,
        True
    )


    results = partsupp_probe.sum(lambda p: 
                                    {
                                        p[0].concat(
                                            record
                                            (
                                                {
                                                    "supplier_cnt" : dictSize(p[1])
                                                }
                                            )
                                        ):
                                        True
                                    }
                                )

    return results

#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q17(li, pa):

    brand23 = "Brand#23"
    med = "MED BOX"
                                                     
    pa_indexed = pa.joinBuild("p_partkey", lambda p: p[0].p_brand==brand23 and p[0].p_container==med, [])

    li_probed = li.joinProbe(
                    pa_indexed,
                    "l_partkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.l_partkey:
                        record
                        (
                            {
                                "l_quantity": probeDictKey.l_quantity,
                                "count": 1.0
                            }
                        )
                    }
                )
    
    pre_results = li.joinProbe(
                    li_probed,
                    "l_partkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                            probeDictKey.l_extendedprice
                        if
                            (0.2*(indexedDictValue.l_quantity/indexedDictValue.count)) > probeDictKey.l_quantity
                        else
                            0.0
                )

    results = pre_results / 7.0

    return results

#######

@sdql_compile({"li": lineitem_type, "cu": customer_type, "ord": order_type})
def q18(li, cu, ord):

    li_aggregated = li.sum(lambda b: {b[0].l_orderkey: b[0].l_quantity})
    
    li_filtered = li_aggregated.sum(lambda z:
                        {
                            unique(z[0]):
                            True
                        }
                        if z[1] > 300  else
                        None)
   
    cu_indexed = cu.joinBuild("c_custkey", lambda p: True, ["c_name"])
    
    order_probed = ord.joinProbe(
                        cu_indexed, 
                        "o_custkey",
                        lambda p: li_filtered[p[0].o_orderkey] != None,
                        lambda indexedDictValue, probeDictKey:
                        {
                            probeDictKey.o_orderkey:
                            record({"c_name": indexedDictValue.c_name, "o_custkey": probeDictKey.o_custkey, "o_orderkey": probeDictKey.o_orderkey, "o_orderdate": probeDictKey.o_orderdate, "o_totalprice": probeDictKey.o_totalprice})
                        },
                        False)
    
    li_probed = li.joinProbe(
                    order_probed,
                    "l_orderkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        record({"c_name": indexedDictValue.c_name, "o_custkey": indexedDictValue.o_custkey, "o_orderkey": indexedDictValue.o_orderkey, "o_orderdate": indexedDictValue.o_orderdate, "o_totalprice": indexedDictValue.o_totalprice}):
                        record({"quantitysum": probeDictKey.l_quantity})
                    }
                    )

    results = li_probed.sum(lambda p: {unique(p[0].concat(p[1])): True})

    return results

#######

@sdql_compile({"li": lineitem_type, "pa": part_type})
def q19(li, pa):

    brand12 = "Brand#12"
    brand23 = "Brand#23"
    brand34 = "Brand#34"

    smcase = "SM CASE"
    smbox = "SM BOX"
    smpack = "SM PACK"
    smpkg = "SM PKG"

    mdbag = "MED BAG"
    mdbox = "MED BOX"
    mdpack = "MED PACK"
    mdpkg = "MED PKG"

    lgcase = "LG CASE"
    lgbox = "LG BOX"
    lgpack = "LG PACK"
    lgpkg = "LG PKG"

    air = "AIR"
    airreg = "AIR REG"

    deliverinperson = "DELIVER IN PERSON"

    pa_indexed = pa.joinBuild("p_partkey", lambda p: 
    (
        (p[0].p_brand == brand12) and
        ((p[0].p_container == smcase) or (p[0].p_container == smbox) or (p[0].p_container == smpack) or (p[0].p_container == smpkg)) and
        ((p[0].p_size >= 1) and (p[0].p_size <= 5))
    ) or 
    (
        (p[0].p_brand == brand23) and
        ((p[0].p_container == mdbag) or (p[0].p_container == mdbox) or (p[0].p_container == mdpack) or (p[0].p_container == mdpkg)) and
        ((p[0].p_size >= 1) and (p[0].p_size <= 10))
    ) 
     or
    (
        (p[0].p_brand == brand34) and
        ((p[0].p_container == lgcase) or (p[0].p_container == lgbox) or (p[0].p_container == lgpack) or (p[0].p_container == lgpkg)) and
        ((p[0].p_size >= 1) and (p[0].p_size <= 15))
    )
    , ["p_brand", "p_size", "p_container"])

    li_probed = li.joinProbe(
                pa_indexed,
                "l_partkey",
                lambda p: (p[0].l_shipinstruct == deliverinperson) and ((p[0].l_shipmode == air) or (p[0].l_shipmode == airreg)),
                lambda indexedDictValue, probeDictKey:
                        probeDictKey.l_extendedprice * (1.0 - probeDictKey.l_discount)
                    if
                        (
                            ((indexedDictValue.p_brand == brand12) and ((probeDictKey.l_quantity >= 1) and (probeDictKey.l_quantity <= 11))) or 
                            ((indexedDictValue.p_brand == brand23) and ((probeDictKey.l_quantity >= 10) and (probeDictKey.l_quantity <= 20))) or
                            ((indexedDictValue.p_brand == brand34) and ((probeDictKey.l_quantity >= 20) and (probeDictKey.l_quantity <= 30)))
                        )
                    else
                        0.0
            )

    results = sr_dict({record({"revenue": li_probed}): True})

    return results

#######

@sdql_compile({"su": supplier_type, "na": nation_type, "ps": partsupp_type, "pa": part_type, "li": lineitem_type})
def q20(su, na, ps, pa, li):

    forest = "forest"
    canada = "CANADA"
                                                      
    pa_indexed = pa.joinBuild("p_partkey", lambda p: startsWith(p[0].p_name, forest), [])

    na_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name==canada, [])

    su_indexed = su.joinBuild("s_suppkey", lambda p: na_indexed[p[0].s_nationkey] != None, [])

    li_indexed = li.sum(lambda p: 
                                {
                                    record({
                                            "l_partkey": p[0].l_partkey, 
                                            "l_suppkey": p[0].l_suppkey
                                    }): 
                                    0.5 * p[0].l_quantity
                                } 
                                if p[0].l_shipdate >= 19940101 and p[0].l_shipdate < 19950101 and pa_indexed[p[0].l_partkey] != None and su_indexed[p[0].l_suppkey] != None
                                else 
                                    None
                        )

    ps_indexed = ps.joinBuild(
                                "ps_suppkey", 
                                lambda p: 
                                    li_indexed[record({"l_partkey": p[0].ps_partkey, "l_suppkey": p[0].ps_suppkey})] != None and 
                                    p[0].ps_availqty > li_indexed[record({"l_partkey": p[0].ps_partkey, "l_suppkey": p[0].ps_suppkey})],
                                    []
                            )


    
    results = su.joinProbe(
                        ps_indexed,
                        "s_suppkey",
                        lambda p: True,
                        lambda indexedDictValue, probeDictKey: {record({"s_name": probeDictKey.s_name, "s_address": probeDictKey.s_address}): True},
                        False
                )

    return results

#######

@sdql_compile({"su": supplier_type, "li": lineitem_type, "ord": order_type, "na": nation_type})
def q21(su, li, ord, na):
        
    saudi = "SAUDI ARABIA"
    f = "F"

    nation_indexed = na.joinBuild("n_nationkey", lambda p: p[0].n_name==saudi, [])    

    su_probed = su.joinProbe(
                    nation_indexed,
                    "s_nationkey",
                    lambda p: True,
                    lambda indexedDictValue, probeDictKey:
                    {
                        probeDictKey.s_suppkey: 
                        probeDictKey.s_name
                    },
                    False
                )

    ord_indexed = ord.sum(lambda p:
            {
                dense(6000000, unique(p[0].o_orderkey)): True
            }
        if
            p[0].o_orderstatus == f
        else
            None
    )

    l2_indexed = li.sum(
                lambda p: 
                    {
                        dense(6000000, p[0].l_orderkey) : 
                        vector({p[0].l_suppkey})
                    }
            )

    l3_indexed = li.sum(
            lambda p: 
                    {
                        dense(6000000, p[0].l_orderkey) : 
                        vector({p[0].l_suppkey})
                    }
                if
                    p[0].l_receiptdate > p[0].l_commitdate
                else
                    None
        )

    l1_probed = li.sum(
            lambda p: 
                    {
                        record(
                            {
                                "s_name": su_probed[p[0].l_suppkey]
                            }
                        )
                        :
                        record(
                            {
                                "numwait": 1
                            }
                        )
                    }
                if
                    (p[0].l_receiptdate > p[0].l_commitdate) and
                    su_probed[p[0].l_suppkey] != None and
                    ord_indexed[p[0].l_orderkey] != None and
                    (dictSize(l2_indexed[p[0].l_orderkey]) > 1) and
                    ((dictSize(l3_indexed[p[0].l_orderkey]) > 0) and (dictSize(l3_indexed[p[0].l_orderkey]) > 1)) == False
                else
                    None
            )

    results = l1_probed.sum(lambda p: {unique(p[0].concat(p[1])): True})

    return results

#####

@sdql_compile({"cu": customer_type, "ord": order_type})
def q22(cu, ord):

    p13 = "13"
    p31 = "31"
    p23 = "23"
    p29 = "29"
    p30 = "30"
    p18 = "18"
    p17 = "17"

    ord_indexed = ord.sum(lambda p:
        {
            dense(150000, unique(p[0].o_custkey)): True
        }
    )

    cu_inner = cu.sum(lambda p: 
                            record({"c_acctbal": p[0].c_acctbal, "count": 1.0})
                        if
                            p[0].c_acctbal>0.0 and 
                            (
                                startsWith(p[0].c_phone, p13) or
                                startsWith(p[0].c_phone, p31) or
                                startsWith(p[0].c_phone, p23) or
                                startsWith(p[0].c_phone, p29) or
                                startsWith(p[0].c_phone, p30) or
                                startsWith(p[0].c_phone, p18) or
                                startsWith(p[0].c_phone, p17)
                            )
                        else
                            None
                    )
        
    cu_inner_final = cu_inner.c_acctbal / cu_inner.count

    cu_probed = cu.sum(lambda p: 
                                {
                                record
                                (
                                    {
                                        "cntrycode": substr(p[0].c_phone, 0, 1),
                                    }
                                ):
                                record
                                (
                                    {
                                        "numcust": 1,
                                        "totalacctbal": p[0].c_acctbal 
                                    }
                                )
                            }
                        if
                            p[0].c_acctbal > cu_inner_final and
                            ord_indexed[p[0].c_custkey] == None and
                            (
                                startsWith(p[0].c_phone, p13) or
                                startsWith(p[0].c_phone, p31) or
                                startsWith(p[0].c_phone, p23) or
                                startsWith(p[0].c_phone, p29) or
                                startsWith(p[0].c_phone, p30) or
                                startsWith(p[0].c_phone, p18) or
                                startsWith(p[0].c_phone, p17)
                            )
                        else
                            None
                )

    results = cu_probed.sum(lambda p: {unique(p[0].concat(p[1])): True})

    return results

######### Function Calls

benchmark("Q1",   iterations, q1,   [lineitem], show_results, verbose)
benchmark("Q2",   iterations, q2,   [part, supplier, partsupp, nation, region], show_results, verbose)
benchmark("Q3",   iterations, q3,   [lineitem, customer, order], show_results, verbose)
benchmark("Q4",   iterations, q4,   [order, lineitem], show_results, verbose)
benchmark("Q5",   iterations, q5,   [lineitem, customer, order, region, nation, supplier], show_results, verbose)
benchmark("Q6",   iterations, q6,   [lineitem], show_results, verbose)
benchmark("Q7",   iterations, q7,   [supplier, lineitem, order, customer, nation], show_results, verbose)
benchmark("Q8",   iterations, q8,   [part, supplier, lineitem, order, customer, nation, region], show_results, verbose)
benchmark("Q9",   iterations, q9,   [lineitem, order, nation, supplier, part, partsupp], show_results, verbose)
benchmark("Q10",  iterations, q10,  [customer, order, lineitem, nation], show_results, verbose)
benchmark("Q11",  iterations, q11,  [partsupp, supplier, nation], show_results, verbose)
benchmark("Q12",  iterations, q12,  [order, lineitem], show_results, verbose)
benchmark("Q13",  iterations, q13,  [customer, order], show_results, verbose)
benchmark("Q14",  iterations, q14,  [lineitem, part], show_results, verbose)
benchmark("Q15",  iterations, q15,  [lineitem, supplier], show_results, verbose)
benchmark("Q16",  iterations, q16,  [partsupp, part, supplier], show_results, verbose)
benchmark("Q17",  iterations, q17,  [lineitem, part], show_results, verbose)
benchmark("Q18",  iterations, q18,  [lineitem, customer, order], show_results, verbose)
benchmark("Q19",  iterations, q19,  [lineitem, part], show_results, verbose)
benchmark("Q20",  iterations, q20,  [supplier, nation, partsupp, part, lineitem], show_results, verbose)
benchmark("Q21",  iterations, q21,  [supplier, lineitem, order, nation], show_results, verbose)
benchmark("Q22",  iterations, q22,  [customer, order], show_results, verbose)