from jobmon.attributes.attribute_dictionary import AttributeDictionary

workflow_attribute_types = AttributeDictionary({
    'NUM_LOCATIONS':AttributeDictionary({'ID':1,'TYPE':'int'}),
    'NUM_DRAWS':AttributeDictionary({'ID':2,'TYPE':'int'}),
    'NUM_AGE_GROUPS':AttributeDictionary({'ID':3,'TYPE':'int'}),
    'NUM_YEARS':AttributeDictionary({'ID':4,'TYPE':'int'}),
    'NUM_RISKS':AttributeDictionary({'ID':5,'TYPE':'int'}),
    'NUM_CAUSES':AttributeDictionary({'ID':6,'TYPE':'int'}),
    'NUM_SEXES': AttributeDictionary({'ID': 7, 'TYPE': 'int'})})