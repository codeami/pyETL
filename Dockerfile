FROM python:3.7.7-buster

RUN mkdir /pytest
COPY . /pytest

RUN cd /pytest

ADD odbcinst.ini /etc/odbcinst.ini
RUN apt-get update
RUN apt-get install -y tdsodbc unixodbc-dev
RUN apt install unixodbc-bin -y
RUN apt-get clean -y
RUN apt-get install vim -y

RUN cd /pytest
#RUN pip3  install -r requirements.txt
RUN wget https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-basic-19.6.0.0.0-1.x86_64.rpm
RUN wget https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-devel-19.6.0.0.0-1.x86_64.rpm
RUN wget https://download.oracle.com/otn_software/linux/instantclient/19600/oracle-instantclient19.6-sqlplus-19.6.0.0.0-1.x86_64.rpm
RUN apt-get install alien -y
RUN alien -i oracle-instantclient*-basic-*.rpm
RUN alien -i oracle-instantclient*-devel-*.rpm
RUN alien -i oracle-instantclient*-sqlplus-*.rpm
RUN apt-get install libaio1

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN echo 'deb [arch=amd64] https://packages.microsoft.com/ubuntu/18.04/prod bionic main' | tee /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update
RUN ACCEPT_EULA=Y apt-get -y install msodbcsql17

RUN cd /pytest && pip install -r requirements.txt