FROM python:3.6-buster

COPY webcontent /webcontent

COPY serve.py create_files.py /app/

WORKDIR /webcontent

CMD [ "python", "/app/serve.py" ]
