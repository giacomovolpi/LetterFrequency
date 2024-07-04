# compile
With Maven set up and the provided `pom.xml` file, run

```bash
mvn clean package
```

# run
Copy the `.jar` on the cluster and run

```bash
hadoop jar letterFrequency.jar it.unipi.hadoop.letterFrequencyClass <hdfs_input_file> <hdfs_output_file> [number_of_reducers]
```

## using the launch script

To automatize the execution with logs and timing, we developed a bash script to easily launch multiple versions of the Hadoop implementation.
To use it, run

```bash
./launch.sh <name_of_version_to_test> <input_file> [number_of_reducers]
```

This will automatically compile, transfer the jar and run the MapReduce job. It will collect verbose logs and append the output of the `time` utility installed on the server into a file inside the `logs/` folder.
Finally, it will show the Letter Frequency result to stdout.
