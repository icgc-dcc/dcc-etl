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

# scripts to helps formatting a json file in a condensed form
# usage: ./format.sh my_json_file
# note: does not work via stdin (json.tool doesn't seemt to support it)
python -mjson.tool ${1?} \
 | tr '\n' '\0' \
 | awk '{gsub(/, \0                                    "/,", \"")}1' \
 | awk '{gsub(/, \0                        "/,", \"")}1' \
 | awk '{gsub(/, \0            "/,", \"")}1' \
 | awk '{gsub(/{\0                                    "/,"{ \"")}1' \
 | awk '{gsub(/{\0                        "/,"{ \"")}1' \
 | awk '{gsub(/{\0            "/,"{ \"")}1' \
 | awk '{gsub(/\0                                }/," }")}1' \
 | awk '{gsub(/\0                    }/," }")}1' \
 | awk '{gsub(/\0        }/," }")}1' \
 | tr '\0' '\n'
