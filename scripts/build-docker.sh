#! /bin/bash

no_cache=""
images=""
git_tag_arg=""

while [[ $# -gt 0 ]]
do
  arg="$1"

  shift

  case ${arg} in
    --no-cache)
      no_cache="--no-cache"
      ;; 
    --images)
      images="$1"
      shift
      ;;
    --images-all)
      images="common,wps,celery,thredds,edas"
      ;;
    --git-tag)
      git_tag_arg="--build-arg TAG=$1"
      shift
      ;;
  esac
done

function check_command {
  command -v $1 >/dev/null 2>&1 && $2 || $3
}

# check that docker exists
check_command docker "docker --version" "exit 1"
check_command docker-compose "docker-compose --version" "exit 1"

script_dir="$(cd "$(dirname $0)" && pwd)"

# make sure we're in the git repo
cd $script_dir

git_dir="$(git rev-parse --show-toplevel)"

docker_dir="${git_dir}/docker"

IFS="," read -ra images <<< "$images"
for img in "${images[@]}"
do
  docker build -t jasonb/cwt_${img} ${no_cache} ${build_tag_arg} "${docker_dir}/${img}"
done
