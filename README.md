# pyspark

projects based on docker:
https://hub.docker.com/r/jupyter/pyspark-notebook
for non-docker-setups:
pip install findspark

<br>
<br>
use cases

<h3>
  use case 1:<br>
  (financial risk mgmt)<br>
  show the change in PV for each trading desk across software versions v1/v2<br>
  (or dates d1/d2) 
</h3>

files:
pvdiff.ipynb + its exports: .html and .py<br>
https://github.com/a-moscatelli/pyspark/tree/main/dataframe-pivot (jupyter/python)

<h3>
  use case 2:<br>
  (financial risk mgmt)<br>
  mine the most common patterns that occur on PV diffs
</h3>

files:
FPG-FPmining-on-pvdiffs.ipynb + its exports: .html and .py<br>
https://github.com/a-moscatelli/pyspark/tree/main/MLLIB-FPmining (jupyter/python)

<h3>
  swiss-knife routines
</h3>

* https://github.com/a-moscatelli/pyspark/tree/main/short (python)
* https://github.com/a-moscatelli/pyspark/tree/main/pseudo-random-csv-generator (python)
