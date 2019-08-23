# Brings up a database volume in a container
# Some really old databases might need mysql 5.6

logical_name=$1
port=$2
volume=$3

docker run --name $logical_name --volume $volume:/var/lib/mysql -e MYSQL_ROOT_PASSWORD=docker -p $port:3306 mysql:5.7 &