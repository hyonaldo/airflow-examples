source ~/.bashrc

DEFAULT_AIRFLOW_HOME="~/airflow"
eval AIRFLOW_HOME=$DEFAULT_AIRFLOW_HOME

ls $AIRFLOW_HOME/*.pid
if [[ $? != 0 ]]
then
    echo
    echo "Set the correct AIRFLOW_HOME path, intead of '$AIRFLOW_HOME/'"
    echo "If you've set the path correctly, probably the airflow has already stopped. Just try to start the airflow daemons, if that."
    exit 1
fi

echo ---------------------------------------
echo "ps -ef | grep airflow | egrep -v $(basename $0 .sh) | awk '{ print " '$2' " }'"
ps -ef | grep airflow | egrep -v $(basename $0 .sh)

echo
for pid in $(cat `ls $AIRFLOW_HOME/*.pid`);
do
    echo "kill $pid"
    kill $pid
done

echo ---------------------------------------
echo sleep 5
sleep 5

echo ---------------------------------------
echo "ps -ef | grep airflow | egrep -v $(basename $0 .sh) | awk '{ print " '$2' " }'"
ps -ef | grep airflow | egrep -v $(basename $0 .sh)

echo ---------------------------------------
echo airflow scheduler -D
airflow scheduler -D
echo airflow webserver -D
airflow webserver -D

