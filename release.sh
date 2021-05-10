#!/bin/bash

set -euxo pipefail

if [ ! $(git branch | grep \* | cut -d ' ' -f2) == "master" ]; then
    echo "Error: Not on master branch"
    exit 1
fi
git fetch
if (( $(git log HEAD..origin/master --oneline | wc -l) > 0 )); then
    echo "Error: Branch is not up-to-date with remote origin"
    exit 2
fi

CURRENT_VERSION=$(xmlstarlet sel -t -v /_:project/_:version pom.xml | sed -e s/-SNAPSHOT//g)

MAJOR=$(echo $CURRENT_VERSION | cut -d . -f 1 -)
MINOR=$(echo $CURRENT_VERSION | cut -d . -f 2 -)
PATCH=$(echo $CURRENT_VERSION | cut -d . -f 3 -)

case "$@" in
major)
    MAJOR=$((MAJOR+1))
    MINOR=0
    PATCH=0
    ;;
minor)
    MINOR=$((MINOR+1))
    PATCH=0
    ;;
patch)
    # use patch version from snapshot version
    ;;
*)
    echo "Syntax: release.sh TYPE
Vidarr uses semantic versioning. Valid release types: major, minor, patch"
    exit 3
    ;;
esac
RELEASE_VERSION=${MAJOR}.${MINOR}.${PATCH}

git checkout master
git pull
mvn clean install
mvn versions:set -DnewVersion=${RELEASE_VERSION} -DgenerateBackupPoms=false
git commit -a -m "Vidarr ${RELEASE_VERSION} release"
git tag -a v${RELEASE_VERSION} -m "Vidarr ${RELEASE_VERSION} release"
mvn clean install
mvn deploy
mvn versions:set -DnextSnapshot=true -DgenerateBackupPoms=false
git commit -a -m "prepared for next development iteration"
git push origin master
git push origin v${RELEASE_VERSION}

echo VIDARR_VERSION=${RELEASE_VERSION}
