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
const DYNAMODB_TABLENAME = 'news';

const rs = fs.createReadStream(CSV_FILENAME);
const parser = parse.parse({
  columns: true,
  delimiter: ','
}, function(err, data) {

  var split_arrays = [],
    size = 25;

  while (data.length > 0) {
    split_arrays.push(data.splice(0, size));
  }
  data_imported = false;
  chunk_no = 1;

  async.each(split_arrays, function(item_data, callback) {
    const params = {
      RequestItems: {}
    };
    params.RequestItems[DYNAMODB_TABLENAME] = [];
    item_data.forEach(item => {
	  var date = new Date(item['date']);
      var unixTimeStamp = Math.floor(date.getTime() / 1000);
      params.RequestItems[DYNAMODB_TABLENAME].push({
        PutRequest: {
          Item: {
            newsid: {S: item['newsid']},
			tsunix: {N: String(unixTimeStamp)},
			short_description: {S: item['short_description']},
			link: {S: item['link']},
			authors: {S: item['authors']},
			headline: {S: item['headline']},
			category: {S: item['category']},
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