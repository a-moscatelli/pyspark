#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("gen_n_compare").getOrCreate() 
spark


# In[2]:


####
from datetime import datetime
from pytz import timezone
####
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import * 
from pyspark.sql.functions import *
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import MinMaxScaler
####

def print_now():
    #from datetime import datetime
    #from pytz import timezone
    
    format = "%Y-%m-%d %H:%M:%S %Z%z"
    now_utc = datetime.now(timezone('UTC'))
    SGT =  timezone('Asia/Singapore')
    now_local = now_utc.astimezone(SGT)
    print(now_local.strftime(format))

    
def load_csv():
    path ="./"

    # Some csv data
    pnl = spark.read.csv(path+'pnl.csv',inferSchema=True,header=True)
    return pnl

def show_stats(df,input_columns,dependent_var):
    print('just some stats on distributions')
    df.groupBy(dependent_var).count().show()
    df.groupBy(input_columns).count().show()



# In[4]:


print_now()
pnl = load_csv()
pnl.show()
print('showing the relevant cases only....')
pnl.where("calc_delta_pnldiff_z = 1").select('ccy','curve','calc_exp_z',lit('<->').alias('implic'),'calc_delta_pnldiff_z').show()
show_stats(pnl,['ccy', 'curve', 'calc_exp_z'],"calc_delta_pnldiff_z")
#pnl.printSchema()


# In[5]:


#pnl2 = pnl.withColumn("calc_delta_pnldiff_z", pnl["calc_delta_pnldiff_z"].cast(StringType())).withColumn("calc_exp_z", pnl["calc_exp_z"].cast(StringType()))
#pnl2.printSchema()
from pyspark.sql.functions import concat

# https://stackoverflow.com/questions/51325092/pyspark-fp-growth-algorithm-raise-valueerrorparams-must-be-either-a-param
# you cannot have an array in a cell containing 0 multiple times. array items must be unique. so:
pnl2 = pnl.withColumn("ccy", concat(lit("ccy:"),col('ccy'))) \
.withColumn("curve", concat(lit('curve:'),'curve')) \
.withColumn("calc_exp_z", concat(lit('exptoday:'),'calc_exp_z')) \
.withColumn("calc_delta_pnl_nz", concat(lit('dpnlnz:'),'calc_delta_pnldiff_z'))

pnl3 = pnl2.select('calc_ttm','delta_pnl',array('ccy', 'curve', 'calc_exp_z', 'calc_delta_pnl_nz').alias("items"))
#pnl3.printSchema()
pnl3.toPandas()


# ## start of rule-mining

# In[6]:


from pyspark.ml.fpm import FPGrowth
fpGrowth = FPGrowth(itemsCol="items", minSupport=0.2, minConfidence=0.1)
model = fpGrowth.fit(pnl3)


# In[7]:


itempopularity = model.freqItemsets
# ... FutureWarning: Deprecated in 3.0.0. Use SparkSession.builder.getOrCreate() instead
# ... not under my control

itempopularity.createOrReplaceTempView("itempopularity")
# Then Query the temp view
print("Top 20")
dfo = spark.sql("SELECT * FROM itempopularity ORDER BY freq desc")
dfo.printSchema()
dofd=dfo.select('items','freq',size(dfo.items).alias('len'),array_contains(dfo.items, lit("dpnlnz:1")).alias('isdpnlnz')) #.where(  # .collect()
dofd.limit(20).toPandas()


# In[8]:


dofdx = dofd.where('len>=2 and isdpnlnz')
dofdx.toPandas()


# In[10]:


assoc = model.associationRules
assoc.createOrReplaceTempView("assoc")
# Then Query the temp view
print("Top 20")
df2a = spark.sql("SELECT * FROM assoc ORDER BY confidence desc")
df2a.limit(20).toPandas()


# In[11]:


df2b=df2a.select('antecedent','consequent','confidence','lift','support', \
                 size(df2a.antecedent).alias('lenA'),size(df2a.consequent).alias('lenC'), \
                 array_contains(df2a.consequent,'dpnlnz:1').alias('Cisdpnlnz'))
df2b.toPandas()


# In[12]:


dofdx2 = df2b.where('lenC==1 and Cisdpnlnz')
dofdx2.orderBy(col("lift").desc(),col("lenA").asc()).toPandas()


# In[13]:


## .. the most recurring combination of antecedents'values for calc_delta_pnldiff_z=1 seems to be
# exptoday:1
# ccy:jpy


# In[ ]:




