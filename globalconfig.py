# Record the current git hash and/or diff, upon importing data then saving as parquet,
# when saving results, and when saving models.
# This is a good idea to ensure a model and results can be matched to a specific version
# of the code.
RECORD_GIT_HASH = True
RECORD_GIT_DIFF = True

# When data store is initialised, it receives an identity object. Specify whether to 
# save the details of this object along with the data
RECORD_IDENTITY = True

# Block size to use when importing data. Set to smaller values if memory limited
BLOCK_SIZE = '100MB'