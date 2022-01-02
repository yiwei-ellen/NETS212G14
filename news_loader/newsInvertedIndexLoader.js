var AWS = require('aws-sdk');
var crypto = require('crypto'); 
const stemmer = require("stemmer");

AWS.config.update({region:'us-east-1'});
var db = new AWS.DynamoDB();

var fs = require('fs');
var parse = require('csv-parse');
var async = require('async');

const uuid = require('uuid/v4');

const CSV_FILENAME = "news_loader/newsdf.csv";
const DYNAMODB_TABLENAME = 'invertedNews';
const stopWords = ['a', 'all', 'any', 'but' , 'the', 'in', 'is'];
const rs = fs.createReadStream(CSV_FILENAME);
const parser = parse.parse({
  columns: true,
  delimiter: ','
}, function(err, data) {
	rows = []
	data.forEach(item => {
  //Split request into array of queries
  let queries = item['headline'].split(" ");

  //Process queries by lowercasing, removing nonalpha, and stemming
  for (var i = 0; i < queries.length; i++) {
    let term = queries[i];
    term = term.toLowerCase().replace(/[^a-z]/g, '');
    queries[i] = stemmer(term);
  };

  //Filter query for stopwords
  queries = queries.filter((x) => stopWords.indexOf(x) < 0);
  queries = queries.filter(item => item);

  for (var i = 0; i < queries.length; i++) {
    let term = queries[i];
    rows.push({term: term,
			   newsid : item['newsid']})
  };
})

 console.log("rows Loaded");
 console.log(rows.length);

  var split_arrays = [],
    size = 25;

  while (rows.length > 0) {
    split_arrays.push(rows.splice(0, size));
  }
  data_imported = false;
  chunk_no = 1;
  console.log(split_arrays.length)

  async.each(split_arrays, function(item_data, callback) {
    const params = {
      RequestItems: {}
    };
    params.RequestItems[DYNAMODB_TABLENAME] = [];
    item_data.forEach(item => {
      params.RequestItems[DYNAMODB_TABLENAME].push({
        PutRequest: {
          Item: {
            newsid: {S: item['newsid']},
			term: {N: item['term']},
          }
        }
      });
    });

    db.batchWriteItem(params, function(err, res, cap) {
      console.log('done going next');
      if (err == null) {
        console.log('Success chunk #' + chunk_no);
        data_imported = true;
      } else {
        console.log(err);
        console.log('Fail chunk #' + chunk_no);
        data_imported = false;
      }
      chunk_no++;
      callback();
    });

  }, function() {
    // run after loops
    console.log('all data imported....');

  });

});
rs.pipe(parser);