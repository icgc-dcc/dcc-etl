// Do not use directly, use its .sh counterpart

// Connect
dbHost = "***REMOVED***";
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
