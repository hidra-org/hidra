#
# Regular cron jobs for the hidra package
#
0 4	* * *	root	[ -x /usr/bin/hidra_maintenance ] && /usr/bin/hidra_maintenance
