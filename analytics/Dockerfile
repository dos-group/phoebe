# set base image (host OS)
FROM python:3.8

# set the working directory in the container
WORKDIR /app

ENV APP_ENV=PROD

# copy the dependencies file to the working directory
COPY requirements.txt .

# install dependencies
RUN pip install -r requirements.txt

# copy the content of the local app directory to the working directory
COPY app/ .

# command to run on container start
CMD [ "python", "-u", "./main.py" ]