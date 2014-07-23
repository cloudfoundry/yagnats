#!/bin/bash

set -e
set -x

apt-get -qy update
apt-get -qy install golang git bzr

mkdir -p /gopath
chown -R vagrant:vagrant /gopath

su - vagrant -c /vagrant/.vagrant-provision-as-vagrant-user.sh
