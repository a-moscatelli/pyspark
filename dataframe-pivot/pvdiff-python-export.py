#!/usr/bin/env python
# coding: utf-8

# In[1]:


# findspark ...


# In[2]:


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pv_diff_app").getOrCreate() 
spark


# In[3]:


pvdf = spark.read.csv('pv_sample_input_1.csv',inferSchema=True,header=True)


# In[4]:


import pandas as pd
pd.options.display.max_rows = None
pd.options.display.max_columns = None 
pd.options.display.max_colwidth = None
pd.options.display.precision = 2

# as in https://towardsdatascience.com/8-commonly-used-pandas-display-options-you-should-know-a832365efa95


# In[5]:


pvdf.printSchema()


# In[6]:


pvdf = pvdf.where(pvdf['report ccy'] == 'CCY')
# as in https://sparkbyexamples.com/pyspark/pyspark-where-filter/


# In[7]:


pvdf.toPandas()


# In[8]:


pvdf.groupBy('pnl report').pivot("v").sum("PV").toPandas()
# as in https://sparkbyexamples.com/pyspark/pyspark-pivot-and-unpivot-dataframe/


# In[14]:


scope_pddf = pd.DataFrame({
                    'product': ['irs','fra','lfut','repo'],
                    'take': [True,False,False,False]
                    })
 
scope_psdf = spark.createDataFrame(scope_pddf)

# as in https://www.geeksforgeeks.org/creating-a-pyspark-dataframe/ 


# In[15]:


scope_psdf = scope_psdf.where(scope_psdf['take'])


# In[17]:


scope_psdf.show()


# In[18]:


pvdf.join(other=scope_psdf,on='product',how='inner').groupBy('pnl report').pivot("v").sum("PV").toPandas()


# In[ ]:




