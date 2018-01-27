#! /bin/bash --login

# Parse some args
while [[ "$#" -gt 0 ]]
do
  arg=$1

  shift

  case $arg in
    --shell)
      NEWSHELL=$1
      shift
      ;; 
    --project)
      PROJECT=$1
      shift
      ;;
    --author)
      AUTHOR=$1
      shift
      ;;
  esac
done

function usage {
  echo -e "Usage:"
  echo -e "\t--author\t\tGit Author"
  echo -e "\t--project\t\tGit Project"
  echo -e "\t--branch\t\tGit Branch"
}

[[ -z "${AUTHOR}" ]] && usage && exit 0
[[ -z "${PROJECT}" ]] && usage && exit 0
[[ -z "${BRANCH}" ]] || BRANCH="master"

mkdir -p ${HOME}/builds/${AUTHOR}

cd ${HOME}/builds/${AUTHOR}

if [[ ! -e "${HOME}/builds/${AUTHOR}/${PROJECT}" ]]
then
  git clone --depth=1 https://github.com/${AUTHOR}/${PROJECT}
fi

cd ${PROJECT}

echo "y" | ${HOME}/.travis/travis-build/bin/travis version

${HOME}/.travis/travis-build/bin/travis compile > ci.sh

sed -i.bak s/--branch\\\\\=\\\\\'/--branch\\\\\=\\\\\'${BRANCH}/ ci.sh

# Create missing 2.7 virtualenv
if [[ ! -e "${HOME}/virtualenv/python[\"2.7\"]" ]]
then
  pushd ${HOME}/virtualenv

  virtualenv python["2.7"]

  popd
fi

[[ -z "${NEWSHELL}" ]] && exec bash ci.sh || exec $NEWSHELL
