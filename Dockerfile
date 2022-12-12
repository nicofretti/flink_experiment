FROM flink:1.16.0

# install python3: it has updated Python to 3.9 in Debian 11 and so install Python 3.7 from source
# it currently only supports Python 3.6, 3.7 and 3.8 in PyFlink officially.

RUN apt-get update -y && \
apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev liblzma-dev && \
wget https://www.python.org/ftp/python/3.8.0/Python-3.8.0.tgz && \
tar -xvf Python-3.8.0.tgz && \
cd Python-3.8.0 && \
./configure --without-tests --enable-shared && \
make -j6 && \
make install && \
ldconfig /usr/local/lib && \
cd .. && rm -f Python-3.8.0.tgz && rm -rf Python-3.8.0 && \
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

# Copy the plugin jar to the Flink plugins directory
# COPY ./plugins/flink-csv-1.16.0.jar /opt/flink/opt/
#COPY ./plugins/flink-connector-files-1.16.0.jar /opt/flink/opt/

# Copy the requirements
COPY requirements.txt /opt/requirements.txt
RUN pip3 install --upgrade pip && pip3 install -r /opt/requirements.txt

# sudo apt-get install -y liblzma-dev