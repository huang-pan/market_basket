# Crisis Text Line Take Home Assignment
by: Huang Pan

https://github.com/huang-pan/market_basket/blob/main/docs/Market_basket.pdf

## Subtask 3: Documentation.

#### - write a short explanation of how to run the program.

python main.py

#### - explain why it computes the correct result.

The actual algorithm to solve the problem is documented in the comments here: \
https://github.com/huang-pan/market_basket/blob/main/main.py 

I first calculated the correct result by hand from the data_example.csv.\
I then made an initial version of the code (main_initial.py) that calculates the correct result. The initial version didn't take memory constraints into account.\
I then made a second version of the code (main.py) that takes memory constraints into account and compared the results with that of the initial version.

#### - explain why it works within the memory constraints.

The input csv file is split up into chunks with chunk sizes specifiable by INPUT_CHUNK_SIZE.\
We write many small output csv files specified by the unique product combination of 2, with the smaller product number first.\
   This avoids keeping large output dictionaries in memory that keep track of the counts of all unique product combinations of 2.

#### - explain how you would productionize your solution.

Put code on AWS lambda function with set memory limit.\
Use AWS S3 to store input and output files.\
Can call code using orchestration tool like Airflow.

Instead of using a python dictionary and reading / writing the output files to disk, which is very slow, I would use a database.\
   A fast key value store like AWS Elasticache for redis or AWS MemoryDB for redis would be ideal for storing the output results.

## Bonus

Bonus tasks:

#### - generate datasets with different scales, and measure the runtime of your algorithm.
Does it behave as expected?

Yes, my algorithm scales linearly with the number of rows, it has time complexity O(n). See measurements below:

main.py: unique 2 product output combination in output csv file name\
scale = 1, 200k rows input file\
end: 154.57265996932983 seconds

scale = 2, 400k rows input file\
end: 294.7591280937195 seconds

scale = 3, 600k rows input file\
end: 449.5827350616455 seconds --- O(n) time complexity


If we reduce the INPUT_CHUNK_SIZE, it takes longer to create the chunked input files. But the overall algorithm
may be faster.

INPUT_CHUNK_SIZE: 40000 --> 10000\
scale = 1, 200k rows input file\
end: 154.57265996932983 seconds --> 131.88507795333862 seconds

INPUT_CHUNK_SIZE: 40000 --> 2000\
scale = 1, 200k rows input file\
end: 154.57265996932983 seconds --> 127.2197618484497 seconds


The algorithm starts to slow down if we use fewer output files and store more output product combinations per file:

main.py: unique first product in output csv file name\
         slower execution time, > O(n) time complexity, but fewer output files\
         fewer output files means more output product combination counts in each output file\
         the larger the output file, the slower the read / write to the output file

scale = 1, 200k rows input file\
end: 246.62168097496033 seconds

scale = 2, 400k rows input file\
end: 709.4775440692902 seconds ---, 3x scale 1

scale = 3, 600k rows input file\
end: 1273.5828647613525 seconds ---, 5x scale 1


If we use an intermediate dataset to keep track of the counts of all unique product combinations from each basket,
then use this intermediate dataset to generate the output csv files, the execution time is slightly faster.
This is because we have fewer writes to the output csv files. The drawback of this approach is that the intermediate
dataset takes up memory.

main_complex.py

scale = 1, 200k rows input file\
50135 rows in intermediate dataset / dictionary\
end: 126.80779075622559 seconds

scale = 2, 400k rows input file\
102557 rows in intermediate dataset / dictionary\
end: 271.8661630153656 seconds

scale = 3, 600k rows input file\
154569 rows in intermediate dataset / dictionary\
end: 434.54866003990173 seconds


#### - the baskets in the example data are generated randomly, and therefore each product combination appears in approximately the same number of baskets. Would the algorithm still work if this wasn’t the case, and some product combinations would occur much more often than others?

Yes, the algorithm would still work. The algorithm is not dependent on the number of times a product combination appears in the input file. Certain output csvs will have larger counts than others.

#### - how would the problem change if we are also interested in the occurrence of combinations of three products within a basket?

I created a generalized solution that can handle any number of output product combinations.
I tested the solution for NUM_PRODUCT_COMBINATIONS = 3, there are fewer output csv files due to the combinations formula:\
nCr = n! / (r! * (n - r)!)\
where n = number of unique products, r = number of products in each combination
