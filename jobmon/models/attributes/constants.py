from jobmon.models.attributes.attribute_dictionary import AttributeDictionary

workflow_attribute = AttributeDictionary({
    'NUM_LOCATIONS': 1,
    'NUM_DRAWS': 2,
    'NUM_AGE_GROUPS': 3,
    'NUM_YEARS': 4,
    'NUM_RISKS': 5,
    'NUM_CAUSES': 6,
    'NUM_SEXES': 7,
    'TAG': 8,
    'NUM_MEASURES': 9,
    'NUM_METRICS': 10,
    'NUM_MOST_DETAILED_LOCATIONS': 11,
    'NUM_AGGREGATE_LOCATIONS': 12})

workflow_run_attribute = AttributeDictionary({
    'NUM_LOCATIONS': 1,
    'NUM_DRAWS': 2,
    'NUM_AGE_GROUPS': 3,
    'NUM_YEARS': 4,
    'NUM_RISKS': 5,
    'NUM_CAUSES': 6,
    'NUM_SEXES': 7,
    'TAG': 8,
    'NUM_MEASURES': 9,
    'NUM_METRICS': 10,
    'NUM_MOST_DETAILED_LOCATIONS': 11,
    'NUM_AGGREGATE_LOCATIONS': 12
    })

job_attribute = AttributeDictionary({
    'NUM_LOCATIONS': 1,
    'NUM_DRAWS': 2,
    'NUM_AGE_GROUPS': 3,
    'NUM_YEARS': 4,
    'NUM_RISKS': 5,
    'NUM_CAUSES': 6,
    'NUM_SEXES': 7,
    'TAG': 8,
    'NUM_MEASURES': 9,
    'NUM_METRICS': 10,
    'NUM_MOST_DETAILED_LOCATIONS': 11,
    'NUM_AGGREGATE_LOCATIONS': 12,
    'WALLCLOCK': 13,
    'CPU': 14,
    'IO': 15,
    'MAXRSS': 16,
    'USAGE_STR': 17,
    'DISPLAY_GROUP': 18
})

qsub_attribute = AttributeDictionary({
    'NO_EXEC_ID': -99999,
    'UNPARSABLE': -33333
})
