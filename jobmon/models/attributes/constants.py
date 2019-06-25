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
    'NUM_AGGREGATE_LOCATIONS': 12,
    'SLOT_LIMIT_AT_START': 13,
    'SLOT_LIMIT_AT_END': 14})

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

deploy_attribute = AttributeDictionary({
    'SERVER_QDNS': "jobmon-docker-cont-p01.hosts.ihme.washington.edu",
    'SERVER_HOSTNAME': "jobmon-docker-cont-p01",
    'DB_PORT': 10010,
    'SERVICE_PORT': 10011,
    'SLACK_API_URL': 'https://slack.com/api/chat.postMessage',
})

qsub_attribute = AttributeDictionary({
    'NO_EXEC_ID': -99999,
    'UNPARSABLE': -33333
})
