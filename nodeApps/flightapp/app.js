//import express package
var express = require("express");

//import mongodb package
var mongodb = require("mongodb");

//MongoDB connection URL - mongodb://host:port/dbName
var dbHost = "mongodb://localhost:27017/flight";

//DB Object
var dbObject;

//get instance of MongoClient to establish connection
var MongoClient = mongodb.MongoClient;

//Connecting to the Mongodb instance.
//Make sure your mongodb daemon mongod is running on port 27017 on localhost
MongoClient.connect(dbHost, function(err, db){
  if ( err ) throw err;
  dbObject = db;
});

function getData(responseObj){
  //use the find() API and pass an empty query object to retrieve all records
  dbObject.collection("origindestination_airports").find({}).toArray(function(err, docs){
    if ( err ) throw err;
    var airlineArray = [];
    var delays = [];


    for ( index in docs){
      var doc = docs[index];

      var o_airport = doc['Origin_Airport'];
      var o_city = doc['Origin_City'];
      var o_state = doc['Origin_State'];
      var o_country = doc['Origin_Country'];
//Destination_Airport destination
      var d_airport = doc['Destination_Airport'];
      var d_city = doc['Destination_City'];
      var d_state = doc['Destination_State'];
      var d_country = doc['Destination_Country'];

      var airline=doc['Airline Name'];
      var delay=doc['delay'];

      airlineArray.push({"label": airline});
      delays.push({"value" : delay});

    }

    var dataset = [
      {
        "seriesname" : "Flight Delays",
        "data" : delays
      }
    ];

    var response = {
      "dataset" : dataset,
      "categories" : airlineArray
    };
    responseObj.json(response);
  });
}

//create express app
var app = express();

//Defining middleware to serve static files
app.use('/public', express.static('public'));
app.get("/", function(req, res){
  getData(res);
});
//app.get("/", function(req, res){
  //res.render("chart");
//});

app.listen("3300", function(){
  console.log('Server up: http://localhost:3300');
});
