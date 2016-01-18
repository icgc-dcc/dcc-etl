#!/bin/bash -e
#
# Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
#
# This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
# SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
# TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
# OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
# IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

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

