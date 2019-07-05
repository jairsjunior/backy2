FROM jairsjunior/backy2-base

# RUN apt-get update && apt-get -y install build-essential devscripts debhelper debmake python3-setuptools python3-all python3-pytest python3-alembic python3-dateutil python3-prettytable python3-psutil python3-setproctitle python3-shortuuid python3-sqlalchemy python3-boto python3-azure-storage
RUN mkdir -p /src/backy2

WORKDIR /src/backy2

ADD . .
ADD docker/build-base-image/build.sh .

CMD [ "sh", "./build.sh" ]