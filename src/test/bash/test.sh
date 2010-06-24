#!/bin/bash
cd `dirname $0`
function g {
  echo "> g $@" >&2
  ../../../script/gizzmo -Cconfig.yaml "$@" 2>&1
}
function expect {
  diff - "expected/$1" && echo "    success." || echo "    failed." && exit 1
}

# set -ex

if ["$FLOCK_ENV" -eq ""]; then
  FLOCK_ENV=development
fi

for i in 1 2
do
  for type in edges groups
  do
    db="flock_${type}_${FLOCK_ENV}_${i}"
    echo "drop database if exists $db; create database $db; " | mysql -u"$DB_USERNAME" --password="$DB_PASSWORD"
    cat recreate.sql | mysql -u"$DB_USERNAME" --password="$DB_PASSWORD" "$db"
  done
done

for i in {0..9}
do
  g create localhost "table_repl_$i" com.twitter.service.flock.edges.ReplicatingShard
  g create localhost "table_a_$i" com.twitter.service.flock.edges.SqlShard --source-type="INT UNSIGNED" --destination-type="INT UNSIGNED"
  g create localhost "table_b_$i" com.twitter.service.flock.edges.SqlShard --source-type="INT UNSIGNED" --destination-type="INT UNSIGNED"
  g link "localhost/table_repl_$i" "localhost/table_a_$i" 2
  g link "localhost/table_repl_$i" "localhost/table_b_$i" 1
done