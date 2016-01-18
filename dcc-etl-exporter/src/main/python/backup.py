#!/usr/bin/python
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

from org.apache.pig.scripting import *
import os.path, sys
import getopt
import time
sys.path.append(os.path.dirname(sys.argv[0]))
########CONTROL SETTINGS###########
from core import *

####### DATA PROCESSING ###########
def backupData():
  #create backup directory locally
  Pig.fs("mkdir " + "file:///" + root_backup)
  final_out_static = root_out_static + "/" + release
  final_out_dynamic = out_dynamic + "/" + release

  out_backup_static = root_backup + "/static/"
  out_backup_dynamic = root_backup + "/dynamic/"

  Pig.fs("mkdir " + "file:///" + out_backup_static)
  Pig.fs("mkdir " + "file:///" + out_backup_dynamic)

  Pig.fs("get " + final_out_static + " " + out_backup_static);
  Pig.fs("get " + final_out_dynamic + " " + out_backup_dynamic);
  
if __name__ == "__main__":
  try:
    opts, args = getopt.getopt(sys.argv[1:],"h:r:",["release="])
  except getopt.GetoptError:
    print 'exporter.py [-r <release>]'
    sys.exit(2)
    
  dataTypes = []
  for opt, arg in opts:
    if opt == '-h':
      print 'exporter.py [-r <release>]' 
    elif opt in ("-r", "--release"):
       release = arg

  backupData()
