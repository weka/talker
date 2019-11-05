# You should run this script when you want to publish a new talker version.
# Notice: you must first merge your changes to master branch, then run the script with the new version as a parameter.
# for example: ./publish.sh 1.8.2
set -ex

version=$1

if [ -z "$version" ]
then
      echo "please pass version to the script!"
      exit 0
fi

# update CHANGELOG.md
new_header="## [$version] - $(date +'%Y-%m-%d')"
awk -v line="$new_header" '/Unreleased/ { print; printf "\n"; print line; next }1' CHANGELOG.md > tmpfile
mv tmpfile CHANGELOG.md

git checkout master
git fetch -p
git reset --hard origin/master
echo -n "$version" > VERSION
git add VERSION CHANGELOG.md
git commit -m "version $version"
git tag v"$version"
git push --set-upstream origin master
git push --tags
