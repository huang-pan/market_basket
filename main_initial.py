'''
Crisis Text Line Take Home Assignment
Initial solution without output file splitting that generates the correct solution


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

Subtask 3: Documentation.
- write a short explanation of how to run the program.
- explain why it computes the correct result.
- explain why it works within the memory constraints.
- explain how you would productionize your solution.
'''
# dict of sets of product combinations of 2 occurences for each basket
# sequential loop to scan through the data file
# keep track of the current basket
import csv
import gzip
import time
import os
import glob

## Global constants
INPUT_CSV = 'data/data_example.csv.gz'
CHUNK_SIZE = 25
#INPUT_CSV = 'data/data_1.csv.gz'
#CHUNK_SIZE = 40000
NUM_PRODUCT_COMBINATIONS = 2 # number of product combinations to find
OUTPUT_HEADER = ['product_1', 'product_2', 'num_baskets'] # for NUM_PRODUCT_COMBINATIONS = 2
#OUTPUT_HEADER = ['product_1', 'product_2', 'product_3', 'num_baskets'] # for NUM_PRODUCT_COMBINATIONS = 3

## Initialize global variables
chunk_file_list = [] # list of csv chunk files
current_basket = None # keeps track of current basket
current_products = set() # keeps track of all products in current basket
basket_products = dict() # keeps track of all products in each basket, for DEBUG ONLY
all_products_count = dict() # counts number of basket occurences of sets of similar products from all baskets
final_products_count = dict() # counts number of basket occurences of all product combinations of 2


start_time = time.time()

## Split input csv into multiple files
# https://mungingdata.com/python/split-csv-write-chunk-pandas/
def write_chunk(part, lines):
    file_name = 'data/data_chunk_part_'+ str(part) +'.csv'
    if file_name not in chunk_file_list:
        chunk_file_list.append(file_name)
    with open(file_name, 'w') as f_out:
        f_out.writelines(lines)

with gzip.open(INPUT_CSV, 'rt') as f:
    count = 0
    lines = []
    for line in f:
        count += 1
        lines.append(line)
        if count % CHUNK_SIZE == 0:
            write_chunk(count // CHUNK_SIZE, lines)
            lines = []
    # write remainder
    if len(lines) > 0:
        write_chunk((count // CHUNK_SIZE) + 1, lines)

## Read chunked csvs
# https://docs.python.org/3/library/csv.html
# https://www.pythonforbeginners.com/files/with-statement-in-python
#   Don't need to close file if you use with
new_basket = True
for file_name in chunk_file_list:
    print(file_name)
    with open(file_name) as csvfile:
        mycsv = csv.reader(csvfile)
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
                # write to all_products_count dict
                if current_basket is not None and len(current_products) >= NUM_PRODUCT_COMBINATIONS:
                    # https://stackoverflow.com/questions/59933892/set-as-dictionary-key
                    #current_products_all = frozenset(current_products)
                    #print('c', current_products, sorted(current_products), current_products_all) # sorted doesn't work before frozenset
                    current_products_all = tuple(sorted(current_products))
                    basket_products[current_basket] = current_products_all
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
    basket_products[current_basket] = current_products_all
    all_products_count[current_products_all] = all_products_count.get(current_products_all, 0) + 1

# DEBUG
#print(basket_products)
#print(len(basket_products))
#print('\n')
#print(all_products_count)
print(len(all_products_count))
maxlen = 0
for k, v in all_products_count.items():
    #print(k, len(k))
    if len(k) > maxlen:
        maxlen = len(k)
#print(maxlen) # 5, limited by random.sample(products, random.randint(1, 5)) in generate_data.py
              # nCr: 5C2 = 5! / 2! * (5 - 2)! = 10
print('\n')
#for k, v in all_products_count.items():
#    if v > 1:
#        print(k, v)


## calculate product combinations of NUM_PRODUCT_COMBINATIONS
# https://www.geeksforgeeks.org/combinations-in-python-without-using-itertools/
# Function to create combinations 
# without itertools
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
            #l.append(frozenset([m, *p]))
            l.append((m, *p))
            #print('l', l)

    return l

# nCr = n! / r! * (n - r)!
#arr = [1, 2, 3, 4]
arr = ['A', 'B', 'C', 'D'] # [['A', 'B'], ['A', 'C'], ['A', 'D'], ['B', 'C'], ['B', 'D'], ['C', 'D']]
arr = [99, 132, 60, 95] # [[99, 132], [99, 60], [99, 95], [132, 60], [132, 95], [60, 95]]
arr = [16, 8] # [[16, 8]]
arr = [16, 8, 4] # [[16, 8], [16, 4], [8, 4]]
arr = [16, 3, 20, 14, 15] # [[16, 3], [16, 20], [16, 14], [16, 15], [3, 20], [3, 14], [3, 15], [20, 14], [20, 15], [14, 15]]
#print(n_length_combo(arr, 2))
#print('\n')

## loop through all_products_count to find product combinations of 2
for k, v in all_products_count.items():
    #arr = list(k)
    arr_combinations = n_length_combo(k, NUM_PRODUCT_COMBINATIONS)
    #print(arr, arr_combinations)
    # initial count of 1 for each product combination
    for ele in arr_combinations:
        #ele = frozenset(ele)
        #print(ele)
        final_products_count[ele] = final_products_count.get(ele, 0) + 1
    # multiply by v
    for ele in arr_combinations:
        #ele = frozenset(ele)
        #print(ele)
        #print(v)
        final_products_count[ele] = final_products_count.get(ele, 0) * v

#print(final_products_count)
print(len(final_products_count))
print('\n')

# write output csv
# https://www.geeksforgeeks.org/how-to-save-a-python-dictionary-to-a-csv-file/
# https://stackoverflow.com/questions/8685809/writing-a-dictionary-to-a-csv-file-with-one-line-for-every-key-value
with open('output/output.csv', 'w') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(OUTPUT_HEADER) # write header row
    for k, v in final_products_count.items():
        #print(k, v)
        prod = list(k)
        #print(prod, v)
        writer.writerow([prod[0], prod[1], v])
        #writer.writerow([prod[0], prod[1], prod[2], v])

print("--- %s seconds ---" % (time.time() - start_time))
