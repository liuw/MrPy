#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading

from Protocol import SlaveMsg
import MasterDaemon



def main(slaves):
    master_msgd = threading.Thread(target=MasterDaemon.master_msgd, args=(slaves,))
    master_msgd.start()
    master_msgd.join()
