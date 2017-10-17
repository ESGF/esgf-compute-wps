#! /bin/bash

prefix="dump"
backup_dir="./"
compoise_dir="./"
limit=10

while [[ -n "$1" ]]
do
  case "$1" in
    --dir)
      shift
      backup_dir="$1"
      shift
      ;;
    --compose)
      shift
      compose_dir="$1"
      shift
      ;;
  esac
done

echo "Settings:"
echo "Limit: \"$limit\""
echo "Backup directory: \"$backup_dir\""
echo "Compose directory: \"$compose_dir\""
echo ""

files=$(ls $backup_dir | grep ${prefix}_ | sort -r | tail -n+$limit)

for f in ${files}
do
  echo "Removing \"$f\""

  rm -f ${backup_dir}/$f
done

docker-compose -f ${compose_dir}/docker-compose.yml exec postgres pg_dumpall -c -U postgres > ${backup_dir}/${prefix}_`date +%d-%m-%Y"_"%H_%M_%S`.sql
