"""Functions to define how to preprocess various ACS fields. 

General behavior in phase 1 of preprocessing:
- sensitive vars => no change
- boolean => values (1,2) mapped to 1, 0
- decimal => integer representation divided to correct decimal
- categorical (small cardinality) => one hot encoding
- categorical (large cardinality) => no change
"""