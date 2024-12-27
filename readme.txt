Here are the steps to use this model

1. Start all Hadoop daemons and services
	start-all.sh

2. Create a directory
	hadoop fs -mkdir /user/<Your_name>/youtube_data_input

3. In the test.py file, change hadoop username to yours, and also the path.

4. Run the test.py file. If you get a message "Data has been written to HDFS", it means it is a success.

5. Now run analysis.py, after changing the path in the code.

6. If you have done everything right, you will see the results for your search term.