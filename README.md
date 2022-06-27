# pyspark

docker: https://hub.docker.com/r/jupyter/pyspark-notebook

frequently used lines / use cases

<h3>
  use case 1: show the change in PV for each trading desk across versions (or dates) v1/v2
</h3>

files: pvdiff.ipynb + its rendered pvdiff.html

<pre>
  
import pandas as pd
pd.options.display.max_rows = None
pd.options.display.max_columns = None 
pd.options.display.max_colwidth = None
pd.options.display.precision = 2

pvdf = spark.read.csv('pv_sample_input_1.csv',inferSchema=True,header=True)
pvdf.printSchema()
pvdf = pvdf.where(pvdf['report ccy'] == 'CCY')
pvdf.groupBy('pnl report').pivot('v').sum('PV').toPandas()


scope_pddf = pd.DataFrame({
                    'product': ['irs','fra','lfut','repo'],
                    'take': [True,False,False,False]
                    })
 
scope_psdf = spark.createDataFrame(scope_pddf)
scope_psdf = scope_psdf.where(scope_psdf['take'])
scope_psdf.show()
pvdf.join(other=scope_psdf,on='product',how='inner').groupBy('pnl report').pivot("v").sum("PV").toPandas()

</pre>


<h3>
  use case 2: efficiently check if a column-set of two dataframes match across versions (or dates) v1/v2
</h3>

<pre>
# from pyspark.sql.functions import md5, contact_ws, array
df = df.withColumn('hashcolname',md5(concat_ws('#',array(['column1','column2','column3']))))
# now you compare the hashcolname - if different, something within the column group must be different.
# the function above is null-safe (i.e. no need to coalesce nulls)
</pre>
