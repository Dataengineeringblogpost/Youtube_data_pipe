from Fulload import full_load_fun
from IncreamentalLoad import Increamental_Load

def lambda_handler(event, context):
    # Call the full_load function to perform the full load process
    fulload = full_load_fun()
    print(fulload)
    
    ## Call the incremental load function to perform the incremental load process
    #incremental_load = Increamental_Load()
    #print(incremental_load)
