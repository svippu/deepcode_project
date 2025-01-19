Due to time constraints giving a simple approach to run
The table is created using postgres but if it needs to run in mysql please change the
tags column to json
Step 0: install python and the packages from requirements.txt using pip
Step 1: python breached_processor_input - this will create the table breach_data and add the indexes
Step 2: python frontend_queries.py - this is the server http://127.0.0.1:5000 where you can place the queries and also upload the data file
Step 3: process_sample.py can be used to add fake username and password by giving the path of input filename


