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
