'''
Crisis Text Line Take Home Assignment
Attempt to solve the problem using an intermediate dataset approach.
  Intermediate dataset keeps track of the count of all unique product combinations from each basket.
This approach is slightly faster than the main.py approach.
- but the intermediate dataset takes up too much memory


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
#OUTPUT_HEADER = ['product_1', 'product_2', 'product_3', 'num_baskets'] # for NUM_PRODUCT_COMBINATIONS = 3


## Initialize global variables
chunk_file_list = [] # list of csv chunk files
basket_products = dict() # keeps track of all products in each basket, for DEBUG ONLY
all_products_count = dict() # counts number of basket occurences of sets of similar products from all baskets
final_products_count = dict() # counts number of basket occurences of all product combinations of 2


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
        #print(f)
        os.remove(f)    
    files = glob.glob('./output/*.csv')
    for f in files:
        #print(f)
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
                #print(row) # each row is a list of columns of the csv: ['d8436517-bba1-40bb-ab0e-b7f8c114e56b', '19']
                basket, product = row[0], int(row[1])
                #print(basket, product)
                if basket == current_basket:
                    # existing basket
                    new_basket = False
                    current_products.add(product)
                else:
                    # new basket
                    new_basket = True
                    # write to final_products_count dict
                    if current_basket is not None and len(current_products) >= NUM_PRODUCT_COMBINATIONS:
                        # https://stackoverflow.com/questions/59933892/set-as-dictionary-key
                        #current_products_all = frozenset(current_products)
                        current_products_all = tuple(sorted(current_products))
                        #len_cpa = len(current_products_all)
                        #print('c', current_products, sorted(current_products), current_products_all) # sorted doesn't work before frozenset
                        #basket_products[current_basket] = current_products_all # DEBUG
                        all_products_count[current_products_all] = all_products_count.get(current_products_all, 0) + 1
                    # reset current_basket, current_products
                    current_basket = basket
                    current_products = set()
                    current_products.add(product)

    # write last basket
    if new_basket is False and len(current_products) >= NUM_PRODUCT_COMBINATIONS:
        # https://stackoverflow.com/questions/59933892/set-as-dictionary-key
        #current_products_all = frozenset(current_products)
        current_products_all = tuple(sorted(current_products))
        #len_cpa = len(current_products_all)
        #basket_products[current_basket] = current_products_all # DEBUG
        all_products_count[current_products_all] = all_products_count.get(current_products_all, 0) + 1


## Calculate product combinations of NUM_PRODUCT_COMBINATIONS
#  https://www.geeksforgeeks.org/combinations-in-python-without-using-itertools/
#  Function to create combinations 
#  without itertools
def n_length_combo(lst, n):
    if n == 0:
       return [[]]

    l =[]
    for i in range(0, len(lst)):
	
        m = lst[i]
        remLst = lst[i + 1:]
        #print('m', m, 'remLst', remLst)
        remainlst_combo = n_length_combo(remLst, n-1)
        #print('remainlst_combo', remainlst_combo)
        for p in remainlst_combo:
            #print('p', p)
            l.append((m, *p))
            #print('l', l)

    return l


## Loop through all_products_count to find product combinations of 2
def generate_final_product_count():
    # k, v is a generator, low memory footprint
    # https://stackoverflow.com/questions/12270492/make-a-simple-python-dictionary-generator
    # https://docs.python.org/3/library/stdtypes.html#dictionary-view-objects
    for k, v in all_products_count.items():
        #print(k, v)
        arr_combinations = n_length_combo(k, NUM_PRODUCT_COMBINATIONS)
        #print(arr, arr_combinations)
        # initial count of 1 for each product combination
        for combination in arr_combinations:
            #print(combination)
            # multiply by v
            store_output_combinations(combination, v)


## Store output combinations
def store_output_combinations(combination, count):

    combination = tuple(sorted(combination))
    #print(combination)
    # Read output csv into dict if output csv exists
    final_products_count = dict() # counts number of basket occurences of all product combinations of 2
    #file_name = 'output/output_'+ str(combination[0]) +'.csv'
    file_name = 'output/output_'+ str(combination[0]) + '_' + str(combination[1]) +'.csv'
    #file_name = 'output/output_part_1.csv'
    if os.path.exists(file_name):
        with open(file_name, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                #print(row) # {'product_1': '16', 'product_2': '8', 'num_baskets': '1'}
                #key = frozenset((int(row['product_1']), int(row['product_2'])))
                key = tuple([int(row['product_1']), int(row['product_2'])])
                final_products_count[key] = int(row['num_baskets'])
    #print(final_products_count)

    # Update dict
    final_products_count[combination] = final_products_count.get(combination, 0) + count
    #print(final_products_count)

    # Write output csv
    with open(file_name, 'w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(OUTPUT_HEADER) # write header row
        # k, v is a generator, low memory footprint
        # https://stackoverflow.com/questions/12270492/make-a-simple-python-dictionary-generator
        # https://docs.python.org/3/library/stdtypes.html#dictionary-view-objects
        for k, v in final_products_count.items():
            prod = list(k)
            #print(prod, v)
            writer.writerow([prod[0], prod[1], v])


## Main
if __name__ == '__main__':
    # Keep track of time
    start_time = time.time()

    split_input_file()

    read_split_csvs()
    #print(all_products_count)
    print(len(all_products_count))

    generate_final_product_count()
    #print(final_products_count)

    print('--- %s seconds ---' % (time.time() - start_time))
