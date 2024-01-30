

def remove_lines_containing_string(file_path, write_path, string_to_remove):
    # Read the content of the file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Remove lines containing the specified string
    lines = [line for line in lines if not line.startswith(string_to_remove)]

    # Write the modified content back to the file
    with open(write_path, 'w') as file:
        file.writelines(lines)

remove_lines_containing_string('watering-sim_20240125T142315.sql', 'watering-sim_red_20240125T142315.sql', 'Synthetic field')
