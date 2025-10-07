from boto3.dynamodb.conditions import Key

def query_range(table, index_name, hash_attr, value, ini, fin, range_attr):
    cond = Key(hash_attr).eq(value) & Key(range_attr).between(ini, fin)
    return table.query(IndexName=index_name, KeyConditionExpression=cond, ScanIndexForward=False)

def query_latest(table, index_name, hash_attr, value):
    cond = Key(hash_attr).eq(value)
    return table.query(IndexName=index_name, KeyConditionExpression=cond, ScanIndexForward=False, Limit=1)
