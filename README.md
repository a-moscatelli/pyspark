# pyspark

docker: https://hub.docker.com/r/jupyter/pyspark-notebook

frequently used lines / use cases

<h3>
  use case 1: show the change in pv for each report across versions v1/v2
</h3>

see pvdiff.ipynb and its rendered version pvdiff.html

<pre>
  
import pandas as pd
pd.options.display.max_rows = None
pd.options.display.max_columns = None 
pd.options.display.max_colwidth = None
pd.options.display.precision = 2

pvdf.printSchema()
pvdf = pvdf.where(pvdf['report ccy'] == 'CCY')
pvdf.groupBy('pnl report').pivot("v").sum("PV").toPandas()
  
</pre>
