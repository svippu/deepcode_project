import random
import string

# Names to replace in the "name" field
names = ["Patrick", "Robert", "Jennifer", "Roger", "Shiva", "Vishnu"]

# Function to generate a random password
def generate_password(length=8):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

# Function to process the file
def process_sample_file(input_file, output_file):
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        # Process lines here

        for line in infile:
            # Split the line into parts
            parts = line.rsplit(":", 2)  # Split at most twice from the right
            if len(parts) == 3:
                uri, name_placeholder, password_placeholder = parts

                # Replace placeholders with random values
                name = random.choice(names)
                password = generate_password()

                # Write the modified line to the output file
                modified_line = f"{uri}:{name}:{password}\n"
                outfile.write(modified_line)
            else:
                print(f"Skipping malformed line: {line.strip()}")

# Input and output file paths
input_file = "C:\\Users\\Lenovo\\Downloads\\sample\\sample.txt"   # Replace with your input file path
output_file = "processed_sample.txt"  # Replace with your output file path

# Process the file
process_sample_file(input_file, output_file)

print(f"Processing complete. Modified file saved as {output_file}")
