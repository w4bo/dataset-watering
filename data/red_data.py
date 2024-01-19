

def remove_lines_containing_string(file_path, write_path, string_to_remove):
    # Read the content of the file
    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Remove lines containing the specified string
    lines = [line for line in lines if not line.startswith(string_to_remove)]

    # Write the modified content back to the file
    with open(write_path, 'w') as file:
        file.writelines(lines)

remove_lines_containing_string('watering-forecasting-20240119.sql', 'watering-forecasting-20240119-red.sql', 'Synthetic field')
#with open('watering-forecasting-20240119.sql', 'r') as f:
#    with open('watering-forecasting-20240119-red.sql', 'w') as w:
#        w.write(re.sub(r'^Synthetic field *\n?', '', f.read(), flags=re.MULTILINE))

