from pymongo import MongoClient
from pprint import pprint as pp
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import pandas as pd

class Engine(object):
    """docstring for ."""
    attrs = []
    ctrs = {}
    types = {}

    amazon_ctrs = {}
    bestbuy_ctrs = {}
    walmart_ctrs = {}
    # {"attr": [#int, #string, #bool]}
    amazon_types = {}
    bestbuy_types = {}
    walmart_types = {}

    def __init__(self):
        pass

    def appendUnique(self, ls, el):
        for x in ls:
            if x == el:
                return
        ls.append(el)

    def aux(self, doc, field):
        if not doc.get(field):
            return
        if not isinstance(doc.get(field), (list, dict)):
            self.appendUnique(self.attrs, field)
            self.ctrs[field] = (self.ctrs[field] + 1) if field in self.ctrs else 0

            if not field in self.types:
                if isinstance(field, str):
                    self.types[field] = [0, 1, 0]
            else:
                if isinstance(field, str):
                    self.types[field] = [self.types[field][0], self.types[field][1]+1, self.types[field][2]]
        else:
            pass

    def run(self):
        client = MongoClient("mongodb://localhost:27017/")
        db = client["test-database"]
        coll_prods = db["products"]

        cursor = coll_prods.find({})
        ctr = 0
        for p in cursor:
            if ctr == 10001:
                break
            for field in p:
                self.aux(p, field)
            ctr += 1
        pp(self.ctrs)
        pp(self.attrs)
        pp(self.types)
        df = pd.DataFrame(self.ctrs, index=self.attrs)
        pp(df)
        pp(ctr)
        plt.hist(df, 50)
        plt.show()
        plt.plot(df)
        plt.show()
        pp(df.mean())
        pp(df.median())
        pp(df.std())
        pp(df.var())
        stats.mode(df)
        client.close()

e = Engine()
e.run()
