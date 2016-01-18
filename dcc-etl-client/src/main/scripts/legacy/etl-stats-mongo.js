//
// Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.
//
// This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
// You should have received a copy of the GNU General Public License along with
// this program. If not, see <http://www.gnu.org/licenses/>.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
// EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
// SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
// TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
// OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
// IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
// ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Do not use directly, use its .sh counterpart

// Connect
dbHost = "<host>";
db = connect(dbHost + "/" + dbName); // dbName passed as parameter
db.auth(dccusername, dccpasswd); // passed as parameters

// Aggregate donor type counts
donorTypeCounts = db.runCommand({ 
  "aggregate" : "Observation" , 
  "pipeline" : [ 
    { "$project" : { "_id" : 0 , "donorId" : "$_donor_id" , "type" : "$_type" } } , 
    { "$group" : { "_id" : { "donorId" : "$donorId" , "type" : "$type"} , "typeCount" : { "$sum" : 1 } } } , 
    { "$group" : { "_id" : "$_id.donorId" , "typeCounts" : { "$addToSet" : { "type" : "$_id.type" , "typeCount" : "$typeCount" } } } } , 
    { "$project" : { "_id" : 0 , "donorId" : "$_id" , "typeCounts" : 1 } }
  ]
});
print("There are " + donorTypeCounts.result.length + " donors with observations");

// Aggregate uniquely affected gene ids
affectedGeneIds = db.runCommand({ 
  "aggregate" : "Observation" , 
  "pipeline" : [ 
    { "$project" : { "_id" : 0 , "geneIds" : "$consequence._gene_id" } } , 
    { "$unwind" : "$geneIds" }, 
    { "$group" : { "_id" : "$geneIds" } } , 
    { "$project" : { "_id" : 0 , "geneId" : "$_id" } }
  ]
});
print("There are " + affectedGeneIds.result.length + " uniquely affected gene ids");

// Aggregate gene-donor summaries
geneDonors = db.runCommand({ 
  "aggregate" : "Observation", 
  "pipeline" : [ 
    { "$project" : { "_id" : 0 , "donorId" : "$_donor_id" , "type" : "$_type" , "geneIds" : "$consequence._gene_id" } }, 
    { "$unwind" : "$geneIds" }, 
    { "$group" : { "_id" : { "geneId" : "$geneIds" , "donorId" : "$donorId" , "type" : "$type" } } }, 
    { "$group" : { "_id" : { "geneId" : "$_id.geneId" , "donorId" : "$_id.donorId"} , "types" : { "$addToSet" : "$_id.type" } } }, 
    { "$group" : { "_id" : "$_id.geneId" , "donors" : { "$addToSet" : { "donorId" : "$_id.donorId" , "types" : "$types" } } } }, 
    { "$project" : { "_id" : 0 , "geneId" : "$_id" } }
  ]
});
print("There are " + geneDonors.result.length + " genes with affected donors");
