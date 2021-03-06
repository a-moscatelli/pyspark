
# swiss-knife routine 1:
# efficiently check if a column-set of two dataframes match across versions (or dates) v1/v2 

# from pyspark.sql.functions import md5, contact_ws, array

df = df.withColumn('hashcolname',md5(concat_ws('#',array(['column1','column2','column3']))))

# now you compare the hashcolname
# if different, something within the column group must be different.
# the function above is null-safe (i.e. no need to coalesce nulls)
