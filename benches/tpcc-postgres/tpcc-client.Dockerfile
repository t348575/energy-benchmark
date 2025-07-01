FROM mcr.microsoft.com/openjdk/jdk:21-ubuntu
RUN apt-get update && \
    apt-get install -y openssh-server sudo && \
    mkdir /var/run/sshd && \
    sed -i 's/#PermitRootLogin .*/PermitRootLogin yes/' /etc/ssh/sshd_config
COPY src/add-key.sh /
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]