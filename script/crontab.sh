#!/bin/bash
crontab -l > /opt/tmp.txt && cat /data/mysqltoes/mysqltoes-1.0/bin/crontab.txt >> /opt/tmp.txt && crontab /opt/tmp.txt
rm -f /opt/tmp.txt
