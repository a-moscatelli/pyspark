# pyspark

docker:
https://hub.docker.com/r/jupyter/pyspark-notebook

use cases

<h3>
  use case 1:<br>
  financial risk mgmt
  show the change in PV for each trading desk across software versions (or dates) v1/v2
</h3>

files:
pvdiff.ipynb + its exports pvdiff.html and pvdiff.py

<h3>
  use case 2:<br>
  efficiently check if a column-set of two dataframes match across versions (or dates) v1/v2
</h3>

<pre>
# from pyspark.sql.functions import md5, contact_ws, array
df = df.withColumn('hashcolname',md5(concat_ws('#',array(['column1','column2','column3']))))
# now you compare the hashcolname - if different, something within the column group must be different.
# the function above is null-safe (i.e. no need to coalesce nulls)
</pre>
