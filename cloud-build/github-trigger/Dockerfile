FROM python:3.9.2
WORKDIR /cloud-build/github-trigger/
COPY . .
COPY requirements.txt . 
RUN pip install -r requirements.txt
RUN pip install protobuf==3.20.*
EXPOSE 8080
ENV PYTHONPATH "${PYTHONPATH}:/"
