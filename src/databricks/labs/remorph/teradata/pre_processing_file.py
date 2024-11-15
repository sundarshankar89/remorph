import json
import re


# Load and parse regex patterns from config json
def load_and_parse_patterns(config_path):
    with open(config_path, "r") as f:
        json_data = json.load(f)

    patterns = json_data['patterns']
    # Return both the list of patterns and the dictionary of patterns
    return patterns, {p['pattern']: p['replacement'] for p in patterns}


# Method to replace pattern with a matched group
def replacement_function(match, replacement):
    if replacement == 'replace_group_1':
        return match.group(1)
    elif replacement == 'replace_group_2':
        return f"BINARY TITLE \'{match.group(0)}\'"
    elif replacement == 'replace_group_3':
        return f"{match.group(0)} TITLE \'{match.group(0)}\'"
    else:
        return replacement


# Method to pattern match and replace pattern for each line of the sql
def process_sql_line(line, patterns):
    pattern = patterns['pattern']
    replacement = patterns['replacement']
    line = re.sub(pattern, lambda match: replacement_function(match, replacement), line)
    return line


# Function to format columns with $ or # in their names by enclosing them in quotes
def format_column(match):
    col_name, data_type = match.groups()
    if "\"" in col_name:
        return f'{col_name} {data_type}'
    elif '$' in col_name:
        return f'"{col_name}" {data_type}'
    elif '#' in col_name:
        return f'"{col_name}" {data_type}'
    return f'{col_name} {data_type}'


# Get list of column names in DDL
def extract_column_name(content):
    lines = content.split('\n')
    first_words = []
    for line in lines:
        match = re.search(r'\b(\w+)\b', line)
        if match:
            first_words.append(match.group(1))
    return first_words


# Method to extract only column names present in definition of DDL
def find_matching_parenthesis(sql):
    stack = []
    for i, char in enumerate(sql):
        if char == '(':
            stack.append(i)
        elif char == ')':
            if not stack:
                raise ValueError("Mismatched parentheses")
            start = stack.pop()
            if len(stack) == 0:
                return sql[start + 1 : i].strip()  # Extract content between first pair of parentheses
    raise ValueError("Mismatched parentheses")


# Method to generate primary index that will be used as Liquid Clustering keys
# https://docs.databricks.com/en/delta/clustering.html
def generate_primary_index(process_sql):
    # Get all indexes in DDL
    pattern = re.compile(r'(?:UNIQUE|PRIMARY)?\s*(?:UNIQUE|PRIMARY)?\s*\bINDEX\b[^(\n]*\(([^)]+)\)')
    matches = pattern.findall(process_sql)
    output_text = re.sub(pattern, '', process_sql)
    formatted_result = ', '.join(matches)

    ## check if index key is present in DDL
    if formatted_result == "":
        return output_text
    else:
        ## Get first 32 columns in a list as liquid clustering only collects stats for first 32 columns
        try:
            content_between_parentheses = find_matching_parenthesis(process_sql)
            column_names = extract_column_name(content_between_parentheses)
        except ValueError as e:
            print("Error:", str(e))

        # get first 4 index keys as liquid clustering can be done on a max of 4 columns
        all_keys = [key.strip() for key in formatted_result.split(",")][:4]
        columns_list = column_names[:32]

        # get only the index keys that are present in the first 32 columns
        liquid_clustering_keys = list(set(all_keys).intersection(set(columns_list)))
        if liquid_clustering_keys:
            final_keys = ", ".join(liquid_clustering_keys)
            final_ddl = output_text.replace(";", "") + f"PRIMARY INDEX ({final_keys});"
            return final_ddl
        else:
            return output_text


def execute(sql, patterns_list, patterns_dict):
    updated_content = sql  # Initialize with the original SQL for best practice

    # check if it's a table or view
    # table_pattern = re.compile(r'^(CREATE|REPLACE)\s+\w*\s*TABLE\b', re.MULTILINE)
    table_pattern = re.compile(r'^(CREATE|REPLACE).*?\bTABLE\b', re.MULTILINE)
    table_match = table_pattern.search(updated_content)
    if table_match:
        obj_type = 'table'
        # c heck for special characters in column names
        if '$' in updated_content:
            dollar_pattern = r'(["]?[a-zA-Z_$][a-zA-Z0-9_$]*)\s+(["]?[a-zA-Z_$][a-zA-Z0-9_$]*)'
            updated_content = re.sub(dollar_pattern, format_column, updated_content)
        if '#' in updated_content:
            hash_pattern = r'(["]?[a-zA-Z_#][a-zA-Z0-9_#]*)\s+(["]?[a-zA-Z_$][a-zA-Z0-9_#]*)'
            updated_content = re.sub(hash_pattern, format_column, updated_content)
    else:
        obj_type = 'view'

    # Pre-process the DDL by iterating over the patterns in the config json
    for pattern_dict in patterns_list:
        pattern = pattern_dict.get('pattern')
        replacement = pattern_dict.get('replacement')
        pattern_type = pattern_dict.get('type')
        object_type = pattern_dict.get('object_type')

        # Check for object type and apply only corresponding patterns. Some patterns can be applied only on table OR only on view OR on both
        if object_type == obj_type or object_type == 'both':
            if pattern_type == 'entireSQL':
                updated_content = re.sub(pattern, replacement, updated_content)
            else:
                # Process line by line
                updated_content = '\n'.join(
                    process_sql_line(line, pattern_dict) for line in updated_content.split('\n')
                )

    # Generate liquid clustering keys
    final_ddl = generate_primary_index(updated_content)

    return final_ddl
