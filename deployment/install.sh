apt update
apt upgrade -y
apt install python3-pip -y
chown -R ubuntu:ubuntu /srv
chmod 777 -R /srv/
mkdir -p /srv/parquet
mkdir -p /srv/parquet/incoming
mkdir -p /srv/tmp
mkdir -p /srv/python

pip install --upgrade pip
pip install virtualenv
source /srv/python/venv/bin/activate
pip install parqueryd

sudo echo "redis_url = redis://redis.gumm43.ng.0001.euw1.cache.amazonaws.com:6379/0" > /etc/parqueryd.cfg
cd /etc/systemd/system/
cp parqueryd.target /etc/systemd/system/
cp parqueryd.controller.service /etc/systemd/system/
cp parqueryd.downloader@.service /etc/systemd/system/
cp parqueryd.worker@.service /etc/systemd/system/
cp parqueryd.moveparquery.service /etc/systemd/system/

sudo systemctl enable parqueryd.controller
sudo systemctl enable parqueryd.moveparquery
sudo systemctl enable parqueryd.downloader@1
sudo systemctl enable parqueryd.worker@1
sudo systemctl enable parqueryd.worker@2
sudo systemctl enable parqueryd.worker@3
sudo systemctl enable parqueryd.worker@4
sudo systemctl enable parqueryd.worker@5
sudo systemctl enable parqueryd.worker@6
sudo systemctl enable parqueryd.worker@7
sudo systemctl enable parqueryd.worker@8
sudo systemctl enable parqueryd.worker@9
sudo systemctl enable parqueryd.worker@10
sudo systemctl enable parqueryd.worker@11
sudo systemctl enable parqueryd.worker@12
sudo systemctl start parqueryd.controller
sudo systemctl start parqueryd.moveparquery
sudo systemctl start parqueryd.downloader@1
sudo systemctl start parqueryd.worker@1
sudo systemctl start parqueryd.worker@2
