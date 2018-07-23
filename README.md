# etl-finance - ETL pipeline to collect, clean, transform and save financial data

This repository basics of an ETL pipeline. ETL is an integral part in Data Engineering. For the purpose of  ease, I am collecting finance data (structured and unstructured) from various sources. Using the following steps you'll be able to run this ETL pipeline successfuly:

**Steps**
1. Clone the repository 
2. Install Docker 
3. Build and Run the Dockerfile. This will build the image containing MySQL and Python installed will all its dependencies required. The `bash` command will open the bash where you can write normal linux commands 
```sh
$ docker build -t etl_finance && docker exec -it etl_finance bash
```

4. Copy the repository folder to our `etl_finance` docker image. The reason for copying to `./app` is because we created the working directory in our image to be `./app`
```sh
$ docker cp /REPO_FOLDER etl_finance:./app/REPO_FOLDER
```
5. Open MySQL inside the image using the following command and give the password `123456` which we set in Dockerfile while creating the image. 
```sh
:/app# mysql -uroot -p 
```
6. Now create a database `CREATE DATABASE stock_market;`. You can choose any name for your database but then you have to change the name in `credentials.py`
7. This ETL process requires extraction of unstructured news data from New York Times. For this we are using NYTimes API. So go to https://developer.nytimes.com/ and get your own API key. Then copy the key in `credentials.py`.
8. Now you are all set. Run the `etl.py` to run the ETL pipeline.
```sh
:/app# python etl.py
```

**Note: You can the parameters in the etl.py according to your wish. **
