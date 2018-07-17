FROM mysql:5.7

EXPOSE 3306
ENV MYSQL_ROOT_PASSWORD 123456
WORKDIR /app

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip 

COPY ./requirements.txt ./requirements.txt
RUN pip3 install --user -r requirements.txt
