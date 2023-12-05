FROM ubuntu:latest

RUN apt update && apt install -y python3 python3-pip python3-rrdtool

RUN pip3 install --upgrade pip

# Install the requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install -r /tmp/requirements.txt

# Copy the application
COPY main.py /app/main.py
COPY requirements.txt /app/requirements.txt
WORKDIR /app

# Run the application
ENTRYPOINT ["python3", "main.py"]