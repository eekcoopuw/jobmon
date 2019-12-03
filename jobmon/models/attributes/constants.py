from jobmon.models.attributes.attribute_dictionary import AttributeDictionary

task_instance_attribute = AttributeDictionary({
    'WALLCLOCK': 1,
    'CPU': 2,
    'IO': 3,
    'MAXRSS': 4,
    'MAXPSS': 5,
    'USAGE_STR': 6
})

qsub_attribute = AttributeDictionary({
    'NO_EXEC_ID': -99999,
    'UNPARSABLE': -33333
})
