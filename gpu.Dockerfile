# To build: docker build -t videoflow -f gpu.Dockerfile .
# To run: docker run -it videoflow
FROM nvidia/cuda:10.1-cudnn7-devel

ENV DEBIAN_FRONTEND noninteractive
RUN apt-get update && apt-get install -y \
	python3-opencv ca-certificates python3-dev git wget sudo && \
  rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \ 
 pkg-config \
 python-dev \ 
 python-opencv \ 
 libopencv-dev \ 
 python-numpy \ 
 python-pycurl

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