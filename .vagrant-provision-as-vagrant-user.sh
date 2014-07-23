#!/bin/bash

set -e
set -x

ln -svf /vagrant/.vagrant-skeleton/bashrc ~/.bashrc
ln -svf /vagrant/.vagrant-skeleton/bash_profile ~/.bash_profile

source ~/.bashrc

go get -u github.com/apcera/gnatsd
go get -u launchpad.net/gocheck
