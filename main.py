'''
Crisis Text Line Take Home Assignment
by: Huang Pan

Final solution with the best execution time performance
- At least the best performance that I can get with the current algorithm in this file
- I opted for the straightforward approach to the problem given the contraints that 
    neither the input nor the output csv files will fit into memory:
  Read in the input csv.gz file, output smaller csv files set by INPUT_CHUNK_SIZE
  For each smaller csv file, read in the csv file line by line
    Create a list of all the unique products in each individual basket
    Calculate all combinations of 2 from the above list
        nCr = n! / (r! * (n - r)!)
        where n = number of unique products in each basket, r = number of products in each combination
    Store the combinations of 2 into a small output csv file specified by the unique 
        output combination of 2 in the file name. This file keeps track of the count of 
	this particular combination. e.g.: output_1_5.csv
	output_1_5.csv stores the count of number of baskets that bought product 1 and product 5
	the product with the smallest number is always first
    The problem with this approach is the small file problem: too many small files.
- I then compared the output results with the output results from the main_initial.py file
  to ensure correctness.
- The solution I created is generalizable to any number of product combinations (2, 3, etc.)


Subtask 1: Find a good way of splitting the data into multiple datasets.
- generate a dataset with scale 1 using the Python script from the data.tar.gz archive
(python generate_dataset.py --scale 1) that can be used to test the implementation
(alternatively you can also work with the data_example.csv.gz file).
- find a good way to structure the data into intermediate datasets, considering the
computation task.
- write a program that splits the dataset into multiple smaller files.
- for the documentation: explain the chosen approach for splitting the dataset.

Subtask 2: Calculate the co-occurrences for products.
- implement the algorithm that computes the product co-occurrences from the
intermediate files created in subtask 1.
'''
from sys import getsizeof
import os
import glob
import csv
import gzip
import time


## Global constants
INPUT_CSV = 'data/data_example.csv.gz'
INPUT_CHUNK_SIZE = 25
#INPUT_CSV = 'data/data_1.csv.gz'
#INPUT_CHUNK_SIZE = 40000
NUM_PRODUCT_COMBINATIONS = 2 # number of product combinations to find
OUTPUT_HEADER = ['product_1', 'product_2', 'num_baskets'] # for NUM_PRODUCT_COMBINATIONS = 2
#NUM_PRODUCT_COMBINATIONS = 3 # number of product combinations to find
#OUTPUT_HEADER = ['product_1', 'product_2', 'product_3', 'num_baskets'] # for NUM_PRODUCT_COMBINATIONS = 3


## Initialize global variables
chunk_file_list = [] # list of csv chunk files


## Split input csv into multiple files
#  https://mungingdata.com/python/split-csv-write-chunk-pandas/
def write_chunk(part, lines):
    file_name = 'data/data_chunk_part_'+ str(part) +'.csv'
    if file_name not in chunk_file_list:
        chunk_file_list.append(file_name)
    with open(file_name, 'w') as f_out:
        f_out.writelines(lines)

def split_input_file():
    # Delete any previously generated files at start of new run
    files = glob.glob('./data/data_chunk_part_*')
    for f in files:
        os.remove(f)    
    files = glob.glob('./output/*.csv')
    for f in files:
        os.remove(f)

    # Split input csv into multiple files
    with gzip.open(INPUT_CSV, 'rt') as f:
        count = 0
        lines = []
        for line in f:
            count += 1
            lines.append(line)
            if (count % INPUT_CHUNK_SIZE) == 0:
                #print(count, getsizeof(lines))
                write_chunk(count // INPUT_CHUNK_SIZE, lines)
                lines = [] # clear out memory
        # write remainder
        if len(lines) > 0:
            write_chunk((count // INPUT_CHUNK_SIZE) + 1, lines)


## Read chunked csvs
#  https://docs.python.org/3/library/csv.html
#  https://www.pythonforbeginners.com/files/with-statement-in-python
#    Don't need to close file if you use with
def read_split_csvs():
    new_basket = True
    current_basket = None # keeps track of current basket
    current_products = set() # keeps track of all products in current basket
    for file_name in chunk_file_list:
        print(file_name)
        with open(file_name) as csvfile:
            mycsv = csv.reader(csvfile) # list of lists
            for row in mycsv:
                basket, product = row[0], int(row[1])
                if basket == current_basket:
                    # existing basket
                    new_basket = False
                    current_products.add(product)
                else:
                    # new basket
                    new_basket = True
                    # write to final_products_count dict
                    if current_basket is not None and len(current_products) >= NUM_PRODUCT_COMBINATIONS:
                        arr = list(current_products)
                        arr_combinations = n_length_combo(arr, NUM_PRODUCT_COMBINATIONS)
                        for combination in arr_combinations:
                            store_output_combinations(combination)
                    # reset current_basket, current_products
                    current_basket = basket
                    current_products = set()
                    current_products.add(product)
        print('--- %s seconds ---' % (time.time() - start_time))

    # write last basket
    if new_basket is False and len(current_products) >= NUM_PRODUCT_COMBINATIONS:
        arr = list(current_products)
        arr_combinations = n_length_combo(arr, NUM_PRODUCT_COMBINATIONS)
        for combination in arr_combinations:
            store_output_combinations(combination)


## Calculate product combinations of NUM_PRODUCT_COMBINATIONS
#  https://www.geeksforgeeks.org/combinations-in-python-without-using-itertools/
#  Function to create combinations without itertools
def n_length_combo(lst, n):
    if n == 0:
       return [[]]

    l =[]
    for i in range(0, len(lst)):
	
        m = lst[i]
        remLst = lst[i + 1:]
        remainlst_combo = n_length_combo(remLst, n-1)
        for p in remainlst_combo:
            l.append((m, *p))

    return l


## Store output combinations
#  Creates an output csv file for each unique product combination, e.g.: output_1_5.csv
#    output_1_5.csv stores the count of number of baskets that bought product 1 and product 5
#    the product with the smallest number is always first
def store_output_combinations(combination):

    combination = tuple(sorted(combination))
    # Read output csv into dict if output csv exists
    final_products_count = dict() # counts number of basket occurences of all product combinations of 2
    #file_name = 'output/output_'+ str(combination[0]) +'.csv' # slower execution time but fewer output files
    if NUM_PRODUCT_COMBINATIONS == 2:
        file_name = 'output/output_'+ str(combination[0]) + '_' + str(combination[1]) +'.csv'
    elif NUM_PRODUCT_COMBINATIONS == 3:
        file_name = 'output/output_'+ str(combination[0]) + '_' + str(combination[1]) + '_' + str(combination[2]) +'.csv'
    if os.path.exists(file_name):
        with open(file_name, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if NUM_PRODUCT_COMBINATIONS == 2:
                    key = tuple([int(row['product_1']), int(row['product_2'])])
                elif NUM_PRODUCT_COMBINATIONS == 3:
                    key = tuple([int(row['product_1']), int(row['product_2']), int(row['product_3'])])
                final_products_count[key] = int(row['num_baskets'])

    # Update dict
    final_products_count[combination] = final_products_count.get(combination, 0) + 1

    # Write output csv
    with open(file_name, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(OUTPUT_HEADER) # write header row
        # k, v is a generator, low memory footprint
        # https://stackoverflow.com/questions/12270492/make-a-simple-python-dictionary-generator
        # https://docs.python.org/3/library/stdtypes.html#dictionary-view-objects
        for k, v in final_products_count.items():
            prod = list(k)
            if NUM_PRODUCT_COMBINATIONS == 2:
                writer.writerow([prod[0], prod[1], v])
            elif NUM_PRODUCT_COMBINATIONS == 3:
                writer.writerow([prod[0], prod[1], prod[2], v])


## Main
if __name__ == '__main__':
    # Keep track of time
    start_time = time.time()

    # Split input csv into multiple files
    split_input_file()

    # Read chunked csvs, generate output csvs
    read_split_csvs()

    print('end: %s seconds ---' % (time.time() - start_time))
