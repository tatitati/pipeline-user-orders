import json
import jsonschema
from jsonschema import validate

def get_schema():
    with open('../extract-load/schemas/users/1.json', 'r') as file:
        schema = json.load(file)
    return schema

def validate_json(json_data):
    execute_api_schema = get_schema()

    try:
        validate(instance=json_data, schema=execute_api_schema)
    except jsonschema.exceptions.ValidationError as err:
        print(err)
        err = "Given JSON data is InValid"
        return False, err

    message = "Given JSON data is Valid"
    return True, message


jsonData = json.loads('{"id" : 10,"address": "DonOfDen","age":30, "created_at": "2020-12-07 20:06:00", "name": "paco"}')
is_valid, msg = validate_json(jsonData)
print(msg)