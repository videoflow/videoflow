# To build: docker build -t videoflow -f Dockerfile .
# To run: docker run -it videoflow
FROM ubuntu:18.04

# Installing opencv and other dependencies
ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y gnupg2 \
    python3-opencv \
    ca-certificates \
    python3-dev \
    git \
    wget \
    libopencv-dev \
    python3-numpy \ 
    python3-pycurl && \
    rm -rf /var/lib/apt/lists/*

# Installing ffmpeg
# Be aware that ffmpeg license might not be safe for commercial use.
RUN echo "deb http://old-releases.ubuntu.com/ubuntu/ yakkety universe" | tee -a /etc/apt/sources.list
RUN  apt-get update && apt-get install -y \
    libav-tools  \ 
    libjpeg-dev \ 
    libpng-dev \ 
    libtiff-dev \ 
    libjasper-dev \
    ffmpeg \
    pkg-config

# create a non-root user
ARG USER_ID=1000
RUN useradd -m --no-log-init --system  --uid ${USER_ID} appuser -g sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER appuser
WORKDIR /home/appuser

ENV PATH="/home/appuser/.local/bin:${PATH}"
RUN wget https://bootstrap.pypa.io/get-pip.py && \
	python3 get-pip.py --user && \
	rm get-pip.py

# Installing videoflow
RUN git clone https://github.com/videoflow/videoflow.git
RUN pip3 install --user /home/appuser/videoflow --find-links /home/appuser/videoflow

# Command to run example
CMD ["python3", "/home/appuser/videoflow/examples/simple_example1.py"]