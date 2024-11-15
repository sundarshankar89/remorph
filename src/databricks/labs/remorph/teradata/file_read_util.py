


def read_ddl_file(file_path):
    try:
        print(f"Reading from: {file_path}")
        filtered_lines = []
        with open(file_path, 'r') as file:
            for line in file:
                # Strip leading/trailing whitespace from the line
                stripped_line = line.strip()

                # Ignore lines that start with '/' or '*'
                if stripped_line.startswith('/') or stripped_line.startswith('*'):
                    continue

                # Print the line (or process it as needed)
                filtered_lines.append(stripped_line + "\n")
            # output_file.write(stripped_line + '\n')
        return ''.join(filtered_lines)
        # print(f"Filtered content has been written to '{output_file_path}'.")
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
    except Exception as e:
        print(f"An error occurred: {e}")
