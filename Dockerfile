FROM tensorflow/tensorflow:nightly-py3

RUN echo "deb http://security.ubuntu.com/ubuntu xenial-security main" \ 
 | tee -a /etc/apt/sources.list
RUN echo "deb http://ppa.launchpad.net/jonathonf/ffmpeg-3/ubuntu xenial main " \ 
 | tee -a /etc/apt/sources.list \
 && apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 4AB0F789CBA31744CC7DA76A8CF63AD3F06FC659

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y \ 
 pkg-config \
 python-dev \ 
 python-opencv \ 
 libopencv-dev \ 
 libav-tools  \ 
 libjpeg-dev \ 
 libpng-dev \ 
 libtiff-dev \ 
 libjasper-dev \ 
 python-numpy \ 
 python-pycurl \ 
 python-opencv


COPY . /videoflow
RUN pip install /videoflow --find-links /videoflow
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
CMD ["python", "/videoflow/examples/object_detector.py"]