from Fulload import full_load_fun
from IncreamentalLoad import Increamental_Load

def lambda_handler(event, context):
    funll = full_load_fun()
    print(funll)
