from sys import argv
import mmap
from collections import Counter
import string
from pprint import pprint

def count_letters_in_file(file_path):
    # Create a counter to store letter counts
    # letter_counts = {chr(l): 0 for l in range(127)}
    letter_counts = Counter()
    
    # Open the file
    with open(file_path, 'r+b') as f:
        # Memory-map the file, size 0 means whole file
        with mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
            # Iterate over the file content
            for char in mm:
                # Convert bytes to string and to lower case
                try:
                    char = char.decode('ascii')
                except:
                    continue
                # Only count alphabetic characters
                # if char in string.ascii_lowercase:
                letter_counts[char] += 1
    total = 0
    for c in string.ascii_lowercase:
        letter_counts[c] += letter_counts[c.upper()]
        total += letter_counts[c] + letter_counts[c.upper()]

    return {letter : 100 * count / total for letter, count in letter_counts.items() if letter in string.ascii_lowercase}

# Example usage
if len(argv) < 2:
    print("provide a file path")
    quit()
file_path = argv[1]
letter_counts = count_letters_in_file(file_path)
pprint(letter_counts)

