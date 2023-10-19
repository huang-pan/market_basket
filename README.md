# Crisis Text Line Take Home Assignment
by: Huang Pan

https://github.com/huang-pan/market_basket/blob/main/docs/Market_basket.pdf

## Subtask 3: Documentation.

#### - write a short explanation of how to run the program.

python main.py

#### - explain why it computes the correct result.

I made an initial version of the code (main_initial.py) that calculates the correct result. The initial version didn't take memory constraints into account.
I then made a second version of the code (main.py) that takes memory constraints into account and compared the results with that of the initial version.

#### - explain why it works within the memory constraints.

The input csv file is split up into chunks with chunk sizes specifiable by INPUT_CHUNK_SIZE.
We write many small output csv files specified by the unique product combination of 2, with the smaller product number first.
   This avoids keeping large output dictionaries in memory that keep track of the counts of all unique product combinations of 2.

#### - explain how you would productionize your solution.

Put code on AWS lambda function with set memory limit
Use AWS S3 to store input and output files
Can call code using orchestration tool like Airflow

Instead of using a python dictionary and reading / writing it to disk, which is very slow, I would use a database.
   A fast key value store like AWS Elasticache for redis or AWS MemoryDB for redis would be ideal.

## Bonus

Bonus tasks:

#### - generate datasets with different scales, and measure the runtime of your algorithm.
Does it behave as expected?

Yes, my algorithm scales linearly with the number of rows, it has time complexity O(n). See measurements below:

main.py: unique 2 product output combination in output csv file name
scale = 1, 200k rows input file
end: 154.57265996932983 seconds

scale = 2, 400k rows input file
end: 294.7591280937195 seconds

scale = 3, 600k rows input file
end: 449.5827350616455 seconds --- O(n) time complexity

main.py: unique first product in output csv file name
         slower execution time,, > O(n) time complexity, but fewer output files
scale = 1, 200k rows input file
end: 246.62168097496033 seconds

scale = 2, 400k rows input file
end: 709.4775440692902 seconds ---, 3x scale 1

scale = 3, 600k rows input file
end: 1273.5828647613525 seconds ---, 5x scale 1

#### - the baskets in the example data are generated randomly, and therefore each product
combination appears in approximately the same number of baskets. Would the algorithm
still work if this wasn’t the case, and some product combinations would occur much more
often than others?

Yes, the algorithm would still work. The algorithm is not dependent on the number of times 
a product combination appears in the input file. Certain output csvs will be larger than others,
any the execution time will be longer however.

#### - how would the problem change if we are also interested in the occurrence of
combinations of three products within a basket?

I created a generalized solution that can handle any number of output product combinations.
I tested the solution for NUM_PRODUCT_COMBINATIONS = 3
