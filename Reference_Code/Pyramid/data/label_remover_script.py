def convert_comma_to_space(input_file, output_file, split_char=','):
    with open(input_file, 'r') as infile:
        lines = infile.readlines()
    
    with open(output_file, 'w') as outfile:
        for line in lines:
            # Split the line by commas.
            items = line.strip().split(split_char)
            # Remove the last item.
            items = items[1:]
            # Join the items with spaces and write to the output file.
            outfile.write(' '.join(items) + '\n')

input_file = input("Enter the input file path (e.g., iris.data): ")
output_file = input("Enter the output file path (e.g., iris_converted.data): ")
split_char = input("Enter the character to split the lines (default is ','): ") or ','
convert_comma_to_space(input_file, output_file, split_char)

print(f"Converted {input_file} to {output_file}.")
