sed -i -e "s/User=hidra/User=root/" \
    /usr/lib/systemd/system/hidra-control-server@.service

# This replacement is due to systemctl not working properly:
# [root@asap3-p00 hidra]# /bin/systemctl is-active hidra@p00_eiger.hidra.test.service
# active
# [root@asap3-p00 hidra]# sudo su hidra
# [hidra@asap3-p00 hidra]$ /bin/systemctl is-active hidra@p00_eiger.hidra.test.service
# inactive