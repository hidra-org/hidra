pattern2='cmd = \["systemctl", "status", service\]'
replacement2='cmd = ["systemctl", "show", service]'

pattern3='pid_regex = r"Main PID: (\[0-9\]\*)"'
replacement3='pid_regex = r"MainPID=([0-9]*)"'

sed -i -e "s/$pattern2/$replacement2/" \
    /usr/lib/python2.7/site-packages/hidra/utils/utils_general.py \
&& sed -i -e "s/$pattern3/$replacement3/" \
    /usr/lib/python2.7/site-packages/hidra/utils/utils_general.py \
&& sed -i -e "s/User=hidra/User=root/" \
    /usr/lib/systemd/system/hidra-control-server@.service

# the last replacement is due to systemctl not working properly:
# [root@asap3-p00 hidra]# /bin/systemctl is-active hidra@p00_eiger.hidra.test.service
# active
# [root@asap3-p00 hidra]# sudo su hidra
# [hidra@asap3-p00 hidra]$ /bin/systemctl is-active hidra@p00_eiger.hidra.test.service
# inactive