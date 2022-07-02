import csv
import sys
import datetime

# deterministic, pseudo-random generation of composite datatype csv

assert len(sys.argv) == 1+3, chr(32).join( [ 'syntax:', sys.argv[0], 'output_file_name.csv', 'number_of_records_on_top_of_header', 'number_of_fields_on_top_of_id' ])	# 'max_MB_size_anyway


this_file, output_file_name,number_of_records_on_top_of_header, number_of_fields_on_top_of_id = sys.argv		# , max_MB_size_anyway

assert int(number_of_records_on_top_of_header) >= 0, 'invalid value for number_of_records_on_top_of_header'
assert int(number_of_fields_on_top_of_id) >= 0, 'invalid value for number_of_fields_on_top_of_id'

def coltype(colSN):
	assert colSN>0
	types = ['i','f','s','d','b']
	return types[colSN%len(types)]
	
def colname(colSN):
	assert colSN>0
	return '_'.join(['field',coltype(colSN),str(colSN)])

def colvalue(colSN,rowSN):
	assert colSN>=1
	assert rowSN>=0
	
	lambdas = {
		'i' : lambda colSN,rowSN: int(rowSN%(10*colSN)),
		'f' : lambda colSN,rowSN: float(round(rowSN%(10*colSN) * 3.14,2)),
		's' : lambda colSN,rowSN: str('name-'+str(rowSN%(10*colSN))),
		'd' : lambda colSN,rowSN: datetime.datetime(2022, 1+rowSN%11, 1+rowSN%27,0+rowSN%24),
		'b' : lambda colSN,rowSN: (rowSN+colSN)%5 ==0
	}
	
	return lambdas[coltype(colSN)](colSN,rowSN)
	


fieldnames = ['id']+ [ colname(colSN) for colSN in range(1,int(number_of_fields_on_top_of_id)) ]	# List comprehension

with open(output_file_name, 'w', newline='') as csvfile:
    
	writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	writer.writeheader()

	for rowSN in range(int(number_of_records_on_top_of_header)):
		rowdict = {'id': rowSN}
		for colSN in range(1,int(number_of_fields_on_top_of_id)):
			rowdict[ colname(colSN) ] = colvalue(colSN,rowSN)
		writer.writerow(rowdict)

csvfile.close()
