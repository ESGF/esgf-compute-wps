#$!/bin/sh

export CATALINA_HOME="/usr/local/tomcat"
export CATALINA_BASE="/usr/local/tomcat"

CONTENT_ROOT=-Dtds.content.root.path=/usr/local/tomcat/content

JAVA_PREFS_ROOTS="-Djava.util.prefs.systemRoot=$CATALINA_HOME/content/thredds/javaUtilPrefs \
                  -Djava.util.prefs.userRoot=$CATALINA_HOME/content/thredds/javaUtilPrefs"

echo "Java heap init ${JAVA_HEAP_INIT} max ${JAVA_HEAP_MAX}"

NORMAL="-d64 -Xmx${JAVA_HEAP_MAX} -Xms${JAVA_HEAP_INIT} -server -XX:MaxPermSize=256m"
HEAD_DUMP="-XX:+HeapDumpOnOutOfMemoryError"
HEADLESS="-Djava.awt.headless=true"

JAVA_OPTS="$CONTENT_ROOT $NORMAL $MAX_PERM_GEN $HEAP_DUMP $HEADLESS $JAVA_PREFS_ROOT"

export JAVA_OPTS
