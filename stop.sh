brew services stop mosquitto

export CONFLUENT_HOME=/Users/pradyumna/pradyapps/bits/confluent/confluent-7.6.0
export PATH="${CONFLUENT_HOME}/bin:$PATH"
#confluent local services stop
confluent local services kafka stop


