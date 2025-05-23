import json

def fix_json_file(input_file, output_file):
    with open(input_file, 'r') as f:
        data = f.read()

    # Split the data by '}\n' to get individual JSON objects
    json_objects = data.split('}\n')

    # Join the JSON objects with a comma and add '}' back to each object
    fixed_data = ','.join([obj.strip() + '}' for obj in json_objects])

    with open(output_file, 'w') as f:
        f.write(fixed_data)

input_file = 'data/tweets.json'
output_file = 'data/khub.json'

fix_json_file(input_file, output_file)

# def remove_trailing_comma(file_path):
#     with open(file_path, 'r+') as f:
#         # Move the cursor to the end of the file
#         f.seek(0, 2)
#
#         # Move the cursor backwards until a non-whitespace character is found
#         while f.read(1).strip() == "":
#             f.seek(-2, 1)
#
#         # If the first non-whitespace character found is a comma, remove it
#         if f.read(1) == ",":
#             print("found")
#             f.seek(-1, 1)
#             f.truncate()
#
# # Example usage:
# file_path = 'data/fianl_fixed.json'  # Replace 'data.json' with your file path
# remove_trailing_comma(file_path)

