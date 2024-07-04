import string
from sys import argv

def letter_frequency(file_path):
    # Initialize a dictionary to store the frequency of each letter
    frequency = {letter: 0 for letter in string.ascii_lowercase}
    total_letters = 0

    # Open and read the file
    with open(file_path, 'r') as file:
        text = file.read().lower()

    # Count the frequency of each letter and the total number of letters
    for char in text:
        if char in frequency:
            frequency[char] += 1
            total_letters += 1

    # Print the frequency of each letter as a percentage
    for letter, count in frequency.items():
        if total_letters > 0:
            freq_percentage = (count / total_letters) * 100
        else:
            freq_percentage = 0
        print(f"{letter}: {freq_percentage:.2f}% count: {count}")
        
#file_path = input('Enter path of the file: ')
if len(argv) < 2:
    print("provide a file path")
    quit()
file_path = argv[1]
letter_frequency(file_path)
