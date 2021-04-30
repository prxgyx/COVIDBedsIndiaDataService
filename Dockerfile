FROM ubuntu:latest

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
    ca-certificates \
    g++ \
    python3-dev \
    python3-distutils \
    python3-venv

RUN apt-get install -y curl && curl -O https://bootstrap.pypa.io/get-pip.py \
    && python3 get-pip.py

WORKDIR app/

RUN apt-get install -y wget && wget https://github.com/mozilla/geckodriver/releases/download/v0.24.0/geckodriver-v0.24.0-linux64.tar.gz

RUN apt install -y firefox

RUN tar -xvzf geckodriver* && chmod +x geckodriver

RUN mv geckodriver /usr/local/bin/

COPY . .

RUN pip install -r requirements.txt