#!/bin/bash -e
# Re-creates a database that contains the specified user
# usage: ./mongo.sh hmongodb-dev.res admin admin XXX ***REMOVED*** YYY

# ===========================================================================

mongo_server=${1?} && shift

admin_database_name=${1?} && shift
admin_database_user=${1?} && shift
admin_passwd=${1?} && shift

normal_database_name=${1?} && shift
normal_database_user=${1?} && shift
normal_database_user_passwd=${1?} && shift

# ===========================================================================

function connect_to_server() {
 mongo_server=${1?} && shift
 echo "conn = new Mongo('${mongo_server?}')"
}
function connect_to_db() {
 db=${1?} && shift
 echo "db = conn.getDB('${db?}')"
}
function authenticate() {
 db=${1?} && shift
 username=${1?} && shift
 passwd=${1?} && shift
 echo "$(connect_to_db ${db?}); db.auth('${username?}', '${passwd?}')"
}
function authenticate_as_admin_user_in_admin_database() {
 admin_database_name=${1?} && shift
 admin_database_user=${1?} && shift
 admin_passwd=${1?} && shift
 authenticate ${admin_database_name?} ${admin_database_user?} ${admin_passwd?}
}
function drop_user() {
 db=${1?} && shift
 username=${1?} && shift
 echo "$(connect_to_db ${db?}); db.removeUser('${username?}');"
}
function print_status() {
 echo "print(db.getMongo()); print(db.getName()); print(db.getCollectionNames())"
}
function display_mongo_command() {
 command=${1?} && shift
 formatted_command=$(echo "${command?}" | awk '{gsub(/; /,"\n  ")}1')
 echo -e "executing mongo commands:\n\n  ${formatted_command?}\n"
}

# ===========================================================================
echo "Dropping database: '${normal_database_name?}'"

command="$(connect_to_server ${mongo_server?})"
command="${command?}; $(authenticate_as_admin_user_in_admin_database ${admin_database_name?} ${admin_database_user?} ${admin_passwd?})"
command="${command?}; $(connect_to_db ${normal_database_name?})"
command="${command?}; db.dropDatabase()"
command="${command?}; $(print_status);"
display_mongo_command "${command?}"
mongo ${mongo_server?} --quiet --eval "${command?}" || { echo "ERROR"; exit 1; }
echo $?

# ---------------------------------------------------------------------------
echo "Adding user '${normal_database_user?}' to database '${normal_database_name?}' (will also re-create the database as a result)"

command="$(connect_to_server ${mongo_server?})"
command="${command?}; $(authenticate_as_admin_user_in_admin_database ${admin_database_name?} ${admin_database_user?} ${admin_passwd?})"
command="${command?}; $(drop_user ${normal_database_name?} ${normal_database_user?})"
command="${command?}; db.addUser('${normal_database_user?}','${normal_database_user_passwd?}')"
display_mongo_command "${command?}"
mongo ${mongo_server?} --quiet --eval "${command?}" || { echo "ERROR"; exit 1; }
echo $?

# ===========================================================================

exit

