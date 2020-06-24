#!/bin/sh

export CATALINA_HOME="/usr/local/tomcat"
export CATALINA_BASE="/usr/local/tomcat"

CONTENT_ROOT=-Dtds.content.root.path=/usr/local/tomcat/content

JAVA_PREFS_ROOTS="-Djava.util.prefs.systemRoot=$CATALINA_HOME/content/threddsCWT/javaUtilPrefs \
                  -Djava.util.prefs.userRoot=$CATALINA_HOME/content/threddsCWT/javaUtilPrefs"

JAVA_OPTS="$CONTENT_ROOT -d64 -server -Djava.awt.headless=true -XX:+UseContainerSupport -XX:MaxRAMPercentage=80.0"
JAVA_OPTS="$JAVA_OPTS $JAVA_PREFS_ROOT"

export JAVA_OPTS
