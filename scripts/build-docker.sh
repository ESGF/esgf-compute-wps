#! /bin/bash

function help {
  echo -e "Usage: $0 ...\n"
  echo -e "--no-cache: \t\tDisable caching when building docker images"
  echo -e "--images: \t\tCommon separated list of images to build"
  echo -e "--images-all: \t\tBuild all images"
  echo -e "--git-tag: \t\tGithub branch to build, this will be passed to all "
  echo -e "\t\t\timages. If you need separate branches launch multiple commands"
  echo -e "--docker-repo: \t\tThe docker repo to tag the image ${docker_repo}/image-name"
  echo -e "--test-compose: \t\tWill run docker-compose after building"
}

no_cache=""
images=""
git_tag_arg=""
docker_repo=""
test_compose=0

if [[ $# -eq 0 ]]
then
  help

  exit
fi

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
    --docker-repo)
      docker_repo="$1/"
      shift
      ;;
    --test-compose)
      test_compose=1
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
  docker build -t ${docker_repo}cwt_${img} ${no_cache} ${build_tag_arg} "${docker_dir}/${img}"
done

docker_file="docker-compose.dev.yml"

running="$(docker-compose -p test -f ${docker_dir}/${docker_file} ps -q 2>/dev/null | wc -l)"

if [[ "$test_compose" -eq 1 ]]
then
  if [[ "$running" -gt 6 ]]
  then
    docker-compose -p tst -f ${docker_dir}/${docker_file} down -v

    sleep 10
  fi

  docker-compose -p test -f ${docker_dir}/${docker_file} up -d

  sleep 10

  docker-compose -p test -f ${docker_dir}/${docker_file} ps

  sleep 10

  docker-compose -p test -f ${docker_dir}/${docker_file} down -v
fi
