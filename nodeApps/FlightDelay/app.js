// Dependencies
var mongodb = require("mongodb"),
    http = require('http'),
    url = require('url');

var port=1234;
// Set up Mongo
var MyDB = mongodb.Db,
    Server = mongodb.Server;

    // Connect to the MongoDB 'enron' database and its 'emails' collection
    var db = new MyDB("flight", new Server("127.0.0.1", 27017, {}));
    db.open(function(err, n_db) { db = n_db });
    var collection = db.collection("airline_dep_delay");

    // Setup a simple API server returning JSON
    http.createServer(function (req, res) {
      var inUrl = url.parse(req.url, true);
      var airlineid = inUrl.query.airlineid;

      collection.findOne({airline: airlineid}, function(err, item) {
        if(err) {
          console.log("Error:" + err);
          res.writeHead(404);
          res.end();
        }
        if(item) {
          res.writeHead(200, {'Content-Type': 'application/json'});
          res.end(JSON.stringify(item), null, 4);
        }
      });

    }).listen(1337, '127.0.0.1');

    console.log('Server running at http://127.0.0.1:1337/');
