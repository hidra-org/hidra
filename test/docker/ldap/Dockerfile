
FROM osixia/openldap:1.4.0

COPY netgroup.ldif /container/service/slapd/assets/config/bootstrap/ldif/custom/
COPY remove_all.ldif remove_all.ldif
COPY remove_eiger.ldif remove_eiger.ldif
COPY remove_control_client.ldif remove_control_client.ldif
COPY add_all.ldif add_all.ldif