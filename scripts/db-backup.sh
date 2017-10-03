#! /bin/bash

prefix="dump"
backup_dir="./"
limit=10

while [[ -n "$1" ]]
do
  case "$1" in
    --dir)
      shift
      backup_dir="$1"
      shift
      break
  esac
done

echo "Settings:"
echo "Limit: \"$limit\""
echo "Backup directory: \"$backup_dir\""
echo ""

files=$(ls $backup_dir | grep ${prefix}_ | sort -r | tail -n+$limit)
count=$(echo $files | wc -l)

if [[ $count -gt 0 ]]
then
  echo "Removing $count files"

  for f in $files
  do
    echo "Removing file \"$f\""
    rm $f
  done
fi

docker-compose -f ../docker/docker-compose.yml exec postgres pg_dumpall -c -U postgres > $prefix_`date +%d-%m-%Y"_"%H_%M_%S`.sql
