#!/bin/bash
#
# Source this file to setup JAVA_DEBUG_OPTS for remote debugging via VisualVM
#

export JAVA_DEBUG_OPTS="-Dcom.sun.management.jmxremote.port=7091 \
-Dcom.sun.management.jmxremote.rmi.port=7091 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
-Djava.rmi.server.hostname=127.0.0.1"
