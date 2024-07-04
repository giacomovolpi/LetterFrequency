from time import time
from pyspark import SparkConf, SparkContext
from sys import argv
from string import ascii_letters

conf = SparkConf()
conf.setAppName("LetterFrequency")
sc = SparkContext(conf=conf)

if len(argv) < 3:
    print(f"USAGE: {argv[0]} <hdfs-file> <output-path>")
    exit()

hdfs_file = argv[1]
out_path = argv[2]
rdd_file = sc.textFile(f"hdfs:///user/hadoop/{hdfs_file}")

# letter_pairs_rdd = rdd_file.flatMap(lambda line : [char.lower() for char in line if char in ascii_letters]).map(lambda letter: (letter, 1))
char_rdd = rdd_file.flatMap(lambda line: list(line))
letters_rdd = char_rdd.filter(lambda char: char in ascii_letters).map(lambda char: char.lower())
letter_pairs_rdd = letters_rdd.map(lambda letter: (letter, 1))

letter_counts = letter_pairs_rdd.reduceByKey(lambda a, b: a+b)
total_letters = letter_pairs_rdd.count()
letter_frequencies = letter_counts.map(lambda x: (x[0], 100 * x[1] / total_letters))

letter_frequencies.saveAsTextFile(f"hdfs:///user/hadoop/{out_path}")
result = letter_frequencies.collect()
for letter, frequency in result:
    print(f"{letter}: {frequency:.6f} %")

sc.stop()
