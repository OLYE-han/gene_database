import os

def convert_comma_to_space(input_file, output_file, split_char=' '):
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

input_file = 'glove_sample.txt'
for i in range(12):
    num_elements = (2 ** i) * 1000
    output_file = f'glove.{num_elements}.txt'

    os.system(f"head -n {num_elements} glove.840B.300d.txt > {input_file}")
    convert_comma_to_space(input_file, output_file)

    print(f"Written to {output_file}.")
