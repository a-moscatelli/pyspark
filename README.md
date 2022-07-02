# pyspark

docker:
https://hub.docker.com/r/jupyter/pyspark-notebook

use cases

<h3>
  use case 1:<br>
  financial risk mgmt<br>
  show the change in PV for each trading desk across software versions v1/v2<br>
  (or dates d1/d2) 
</h3>

files:
pvdiff.ipynb + its exports: .html and .py<br>
https://github.com/a-moscatelli/pyspark/tree/main/dataframe-pivot

<h3>
  use case 2:<br>
  financial risk mgmt<br>
  mine the most common patterns that occur on PV diffs
</h3>

files:
FPG-FPmining-on-pvdiffs.ipynb + its exports: .html and .py<br>
https://github.com/a-moscatelli/pyspark/tree/main/MLLIB-FPmining

<h3>
  swiss-knife routine 1:<br>
  efficiently check if a column-set of two dataframes match across versions (or dates) v1/v2
</h3>

<pre>
# from pyspark.sql.functions import md5, contact_ws, array
df = df.withColumn('hashcolname',md5(concat_ws('#',array(['column1','column2','column3']))))
# now you compare the hashcolname - if different, something within the column group must be different.
# the function above is null-safe (i.e. no need to coalesce nulls)
</pre>
