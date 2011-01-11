#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Slave use this port to receive commands
SLAVE_CPORT = 9001
# Slave use this port to send data (via httpd)
SLAVE_DPORT = 9002

# Master use this port to receive message from slaves
MASTER_MPORT = 9000

# Global FS, can be NFS or anything else
SHARED_STORAGE = "/path/to/shared/storage"
