var express = require("express")
  , bodyParser = require('body-parser')
  , cassandra = require("cassandra-driver");
  
var client = new cassandra.Client({ contactPoints: [ "cassandra" ] });

var app = express();

app.use(bodyParser.json());

app.post('/keyspace', function(req, res) {
  client.execute("CREATE KEYSPACE IF NOT EXISTS data WITH replication " + 
                 "= {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
    function (err) {
      if (err) {
        console.log("Error creating keyspace", err);
        res.sendStatus(500);
      } else {
        console.log("Keyspace created");
        res.sendStatus(200);
      }
    });
});

app.post("/:project", function (req, res) {
  client.execute("CREATE TABLE IF NOT EXISTS data."+req.params.project+" (" +
                "rowkey text," +
                "colkey text," +
                "value text," +
                "PRIMARY KEY(rowkey, colkey)" +
                ")",
    function (err) {
      if (err) {
        console.log("Error creating project", err);
        res.sendStatus(500);
      } else {
        res.sendStatus(200);
      }
    });
});

app.get("/:project/:rowkey/:colkey", function (req, res) {
  client.execute("select value from data."+req.params.project+" where rowkey=? and colkey=?",
    [ req.params.rowkey, req.params.colkey ], { prepare: true }, function (err, result) {
      if (err || result.rows.length == 0) {
        res.json({ value: null });
      } else {
        res.json({ value: JSON.parse(result.rows[0].value) });
      }
    });
});

app.get("/:project/:rowkey", function (req, res) {
  client.execute("select colkey, value from data."+req.params.project+" where rowkey=? and colkey >= ? and colkey <= ?",
    [ req.params.rowkey, req.query.from, req.query.to ], { prepare: true }, function (err, result) {
      if (err) {
        console.log(err);
        res.json([]);
      } else {
        var r = [];
        for (var i=0; i<result.rows.length; i++) {
          r.push({ name: result.rows[i].colkey, value: result.rows[i].value });
        }
        res.json(r);
      }
    });
});

app.put("/:project/:rowkey/:colkey", function (req, res) {
  client.execute("insert into data."+req.params.project+" (rowkey, colkey, value) values(?, ?, ?)",
    [ req.params.rowkey, req.params.colkey, JSON.stringify(req.body.value) ], { prepare: true }, function (err) {
      if (err) {
        console.log(err);
        res.sendStatus(500);
      } else {
        res.sendStatus(200);
      }
    });
});

app.put("/:project/:rowkey", function (req, res) {
  var batch = [];
  for (var i=0; i<req.body.length; i++) {
    batch.push({ 
      query: "insert into data."+req.params.project+" (rowkey, colkey, value) values(?, ?, ?)",
      params: [ req.params.rowkey, req.body[i].name, JSON.stringify(req.body[i].value) ]
    });
  }
  client.batch(batch, { prepare: true }, function (err) {
    if (err) {
      console.log(err);
      req.sendStatus(500);
    } else {
      req.sendStatus(200);
    }
  });
});

client.connect(function (err) {
  if (err) {
    console.log("Cannot connect to cassandra:", err);
  } else {
    console.log("Cassandra client connected");
    app.listen(5000, function (err) {
      if (err) {
        console.log("Cannot start http server:", err);
      } else {
        console.log("Server started at port 5000");
      }
    });
  }
});
