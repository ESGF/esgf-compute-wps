#$!/bin/sh

export CATALINA_HOME="/usr/local/tomcat"
export CATALINA_BASE="/usr/local/tomcat"
export JAVA_HOME="/usr"

CONTENT_ROOT=-Dtds.content.root.path=/usr/local/tomcat/content

JAVA_PREFS_ROOTS="-Djava.util.prefs.systemRoot=$CATALINA_HOME/content/thredds/javaUtilPrefs \
                  -Djava.util.prefs.userRoot=$CATALINA_HOME/content/thredds/javaUtilPrefs"

NORMAL="-d64 -Xmx4096m -Xms512m -server -ea"
HEAD_DUMP="-xx:+HeapDumpOnOutOfMemoryError"
HEADLESS="-Djava.awt.headless=true"

JAVA_OPTS="$CONTENT_ROOT $NORMAL $MAX_PERM_GEN $HEAP_DUMP $HEADLESS $JAVA_PREFS_ROOT"

export JAVA_OPTS
