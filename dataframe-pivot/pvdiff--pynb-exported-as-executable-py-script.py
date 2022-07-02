#!/usr/bin/env python
# coding: utf-8

# In[1]:


# findspark ...


# In[2]:


# start spark session
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pv_diff_app").getOrCreate() 
spark


# In[3]:


import pandas as pd
pd.options.display.max_rows = None
pd.options.display.max_columns = None 
pd.options.display.max_colwidth = None
pd.options.display.float_format = lambda x: '{:,.2f}'.format(x)
# pd.options.display.precision = 2

# as in https://towardsdatascience.com/8-commonly-used-pandas-display-options-you-should-know-a832365efa95


# In[4]:


#load file
pvdf = spark.read.csv('pv_sample_input_1.csv',inferSchema=True,header=True)
pvdf.printSchema()


# In[5]:


pvccy = pvdf.where(pvdf['report ccy'] == 'CCY')
# as in https://sparkbyexamples.com/pyspark/pyspark-where-filter/
pvccy=pvccy.withColumn("PV",pvdf["PV"].cast('float'))
# just in case:


# In[6]:


pvccy.toPandas()


# In[7]:


pvccy.groupBy('pnl report').pivot("v").sum("PV").toPandas()
# as in https://sparkbyexamples.com/pyspark/pyspark-pivot-and-unpivot-dataframe/


# In[8]:


# filtering via inner join with control table


# In[9]:


scope_pddf = pd.DataFrame({
                    'product': ['irs','fra','lfut','repo'],
                    'take': [True,False,False,False]
                    })
 
scope_psdf = spark.createDataFrame(scope_pddf)

# as in https://www.geeksforgeeks.org/creating-a-pyspark-dataframe/ 


# In[10]:


scope_psdf = scope_psdf.where(scope_psdf['take'])


# In[11]:


scope_psdf.show()


# In[12]:


pvccyj=pvccy.join(other=scope_psdf,on='product',how='inner')

pvt=pvccyj.groupBy('pnl report').pivot("v").sum("PV")


# In[13]:


from pyspark.sql.functions import col
pvt=pvt.withColumn("DELTA",col("v2")-col("v1"))


# In[14]:


pvt.toPandas()


# In[15]:


# NOT IN: showing what I am going to miss with inner join = non -irs
# as in https://www.datasciencemadesimple.com/join-in-pyspark-merge-inner-outer-right-left-join-in-pyspark/


# In[17]:


pvccy.join(other=scope_psdf,on='product',how='anti').toPandas()


# In[ ]:




