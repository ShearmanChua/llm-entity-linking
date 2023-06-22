# Refer to README.md for commonly used PyTorch images
FROM pytorch/pytorch:1.13.0-cuda11.6-cudnn8-runtime

ENV NVIDIA_VISIBLE_DEVICES all
ENV NVIDIA_DRIVER_CAPABILITIES compute,utility

ENV DEBIAN_FRONTEND noninteractive

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE 1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get upgrade -y \
    && apt-get install -y \
    && apt-get -y install make \
    && apt-get install python3-dev -y \
    && apt-get install gcc -y

RUN apt install -y git

RUN pip install -U pip

COPY requirements.txt .
RUN pip install -r requirements.txt
# RUN pip install git+https://github.com/huggingface/transformers

RUN pip install "uvicorn[standard]"

RUN mkdir /gateway && mkdir /gateway/src && mkdir /gateway/data && mkdir /gateway/configs
COPY . /gateway/
WORKDIR /gateway/src