import csv
import sys

assert len(sys.argv) == 1+3, chr(32).join( [ 'syntax:', sys.argv[0], 'output_file_name.csv', 'number_of_records_on_top_of_header', 'number_of_fields_on_top_of_id' ])	# 'max_MB_size_anyway


this_file, output_file_name,number_of_records_on_top_of_header, number_of_fields_on_top_of_id = sys.argv		# , max_MB_size_anyway

assert int(number_of_records_on_top_of_header) >= 0, 'invalid value for number_of_records_on_top_of_header'
assert int(number_of_fields_on_top_of_id) >= 0, 'invalid value for number_of_fields_on_top_of_id'


fieldnames = ['id'] + [ 'field'+str(nf) for nf in range(int(number_of_fields_on_top_of_id)) ]	# List comprehension

with open(output_file_name, 'w', newline='') as csvfile:
    
	writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	writer.writeheader()

	for rowSN in range(int(number_of_records_on_top_of_header)):
		rowdict = {'id': rowSN}
		for colSN in range(int(number_of_fields_on_top_of_id)):
			rowdict['field'+str(colSN)] = '-'.join(['value',str(rowSN),str(colSN)])
		writer.writerow(rowdict)

csvfile.close()
