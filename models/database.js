var AWS = require('aws-sdk');
var crypto = require('crypto');
const stemmer = require("stemmer");

AWS.config.update({ region: 'us-east-1' });
var db = new AWS.DynamoDB();


//DB Lookup to see if entered password is the same as stored password
var myDB_checkPassword = function (username, password, callback) {

  var params = {
    TableName: "users",
    IndexName: 'username-index',
    KeyConditionExpression: "#username = :username",
    ExpressionAttributeNames: {
      "#username": "username",
    },
    ExpressionAttributeValues: {
      ":username": { "S": username }
    }
  };

  db.query(params, function (err, data) {
    if (err || data.Items.length == 0) {
      callback(err, null);
    } else {
      var hash = crypto.pbkdf2Sync(password, data.Items[0].salt.S, 1000, 64, `sha256`).toString(`hex`);
      callback(err, data.Items[0].password.S === hash);
    }
  });
}

function makeRandomId() {
  var time = new Date().getTime();
  var uuid = 'xxxxxxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    var r = (time + Math.random() * 16) % 16 | 0;
    time = Math.floor(time / 16);
    return (c == 'x' ? r : (r & 0x3 | 0x8)).toString(16);
  });
  return uuid;
}


//DB call to create a new account given username, password, fullname
var myDB_createAccount = function (username, password, firstname, lastname,
  email, affiliation, birthday, interests, callback) {
  var paramsSearch = {
    TableName: "users",
    IndexName: 'username-index',
    KeyConditionExpression: "#username = :username",
    ExpressionAttributeNames: {
      "#username": "username",
    },
    ExpressionAttributeValues: {
      ":username": { "S": username }
    },
    ProjectionExpression: "password"
  };

  var salt = crypto.randomBytes(16).toString('hex');
  var hash = crypto.pbkdf2Sync(password, salt, 1000, 64, `sha256`).toString(`hex`);

  var paramsCreate = {
    Item: {
      "userid": {
        S: makeRandomId()
      },
      "username": {
        S: username
      },
      "firstname": {
        S: firstname
      },
      "lastname": {
        S: lastname
      },
      "email": {
        S: email
      },
      "affiliation": {
        S: affiliation
      },
      "birthday": {
        N: birthday
      },
      "interests": {
        SS: interests
      },
      "picture" : {
		S: String(Math.floor(Math.random() * 15))
	  },
      "password": {
        S: hash
      },
      "salt": {
        S: salt
      },
    },
    TableName: 'users',
    ReturnValues: 'NONE'
  }

  //Check if user exists, if not then create, otherwise return error message
  db.query(paramsSearch, function (err, data) {
    if (err) {
      callback(err, null);
    } else if (data.Items.length > 0) {
      callback(err, "taken");
    } else {
      db.putItem(paramsCreate, function (err, data) {
        if (err) {
          callback(err, null)
        }
        else {
          callback(null, 'Success')
        }
      });
    }
  });
}

//Code to get the userid of a given username in the users table
var myDB_getId = function (username, callback) {
  var params = {
    TableName: "users",
    IndexName: 'username-index',
    KeyConditionExpression: "#username = :username",
    ExpressionAttributeNames: {
      "#username": "username",
    },
    ExpressionAttributeValues: {
      ":username": { "S": username }
    },
    ProjectionExpression: "userid"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      if (data.Items[0] === undefined) {
        callback("No result", null)
      } else {
        callback(null, data.Items[0].userid.S)
      }
    }
  });
}

//Function to replace the interests string set of a given userid with a new list of interests
var myDB_updateInterests = function (userid, interests, callback) {
  var params = {
    TableName: "users",
    Key: {
      "userid": {
        S: userid
      }
    },
    UpdateExpression: "set interests = :interests",
    ExpressionAttributeValues: {
      ":interests": {
        SS: interests
      }
    },
    ReturnValues: "UPDATED_NEW"
  };

  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data);
    }
  });
}

//function to update user affiliation given userid and new affiliation
var myDB_updateAffiliation = function (userid, affiliation, callback) {
  var params = {
    TableName: "users",
    Key: {
      "userid": {
        S: userid
      }
    },
    UpdateExpression: "set affiliation = :affiliation",
    ExpressionAttributeValues: {
      ":affiliation": {
        S: affiliation
      }
    },
    ReturnValues: "UPDATED_NEW"
  };

  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data);
    }
  });
}

//function to update user email given userid and new email
var myDB_updateEmail = function (userid, email, callback) {
  var params = {
    TableName: "users",
    Key: {
      "userid": {
        S: userid
      }
    },
    UpdateExpression: "set email = :email",
    ExpressionAttributeValues: {
      ":email": {
        S: email
      }
    },
    ReturnValues: "UPDATED_NEW"
  };

  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data);
    }
  });
}

//function to update user password given userid, old password, and new password
//returns message if successful, returns “incorrect password” or “error” otherwise
var myDB_updatePassword = function (userid, oldPassword, newPassword, callback) {
  var params = {
    TableName: "users",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "password, salt"
  };


  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      var hash = crypto.pbkdf2Sync(oldPassword, data.Items[0].salt.S, 1000, 64, `sha256`).toString(`hex`);
      if (data.Items[0].password.S === hash) {
        var salt = crypto.randomBytes(16).toString('hex');
        var hash = crypto.pbkdf2Sync(newPassword, salt, 1000, 64, `sha256`).toString(`hex`);
        var params = {
          TableName: "users",
          Key: {
            "userid": {
              S: userid
            }
          },
          UpdateExpression: "set password = :password, salt = :salt",
          ExpressionAttributeValues: {
            ":password": {
              S: hash
            },
            ":salt": {
              S: salt
            }
          },
          ReturnValues: "UPDATED_NEW"
        };
        db.updateItem(params, function (err, data) {
          if (err) {
            callback(err, null);
          } else {
            callback(null, "Success");
          }
        });
      } else {
        callback(null, "Incorrect password");
      }
    }
  });
}

//function to get all details of a given userid
var myDB_getUserDetails = function (userid, callback) {
  var params = {
    TableName: "users",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
	  "#st" : "status"
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "firstname, lastname, email, affiliation, #st, birthday, interests, userid, username, picture"
  
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

//function to get sparse details of a given userid
var myDB_getUserDetailsSparse = function (userid, callback) {
  var params = {
    TableName: "users",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
	  "#st" : "status"
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "firstname, lastname, affiliation, userid, username, picture, #st"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

//function to update the status of a given userid
var myDB_updateStatus = function (userid, status, callback) {
  var params = {
    TableName: "users",
    Key: {
      "userid": {
        S: userid
      }
    },
    ExpressionAttributeNames: {
      "#st": "status",
    },
    ExpressionAttributeValues: {
      ":st": { "S": status }
    },
    UpdateExpression: "set #st = :st",
    ReturnValues: "UPDATED_NEW"
  };

  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data);
    }
  });
}

//function to get the status of a given userid
var myDB_getStatus = function (userid, callback) {
  var params = {
    TableName: "users",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "#s",
    ExpressionAttributeNames : {'#s': 'status'}
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items[0].status.S);
    }
  });
}

var myDB_addFriend = function (user1, user2, callback) {
  var params1 = {
    TableName: "friends",
    Item: {
      userid: { S: user1 },
      useridfriend: { S: user2 }
    }
  };
  var params2 = {
    TableName: "friends",
    Item: {
      userid: { S: user2 },
      useridfriend: { S: user1 }
    }
  };
  db.putItem(params1, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      db.putItem(params2, function (err, data) {
        if (err) {
          callback(err, null);
        } else {
          callback(null, "Success");
        }
      });
    }
  });
}

//function to remove a user1-user2 edge given user1 and user2
var myDB_removeFriend = function (user1, user2, callback) {
  var params1 = {
    TableName: "friends",
    Key: {
      userid: { S: user1 },
      useridfriend: { S: user2 }
    }
  };
  var params2 = {
    TableName: "friends",
    Key: {
      userid: { S: user2 },
      useridfriend: { S: user1 }
    }
  };
  db.deleteItem(params1, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      db.deleteItem(params2, function (err, data) {
        if (err) {
          callback(err, null);
        } else {
          callback(null, "Success");
        }
      });
    }
  });
}

//function to get all the friends of a given userid
var myDB_getFriends = function (userid, callback) {
  var params = {
    TableName: "friends",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "useridfriend"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

//function to get affiliation and first and last name of a given userid
var myDB_getUserAffiliationName = function (userid, callback) {
  var params = {
    TableName: "users",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "firstname, lastname, affiliation"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}


//function to get the friends of friends of a userid with the same affiliation
var myDB_getFriendsOfFriends = function (userid, limit, callback) {
  myDB_getUserAffiliationName(userid, function (err, dataOuter) {
    if (err) {
      callback(err, null);
    } else {
      var userAffiliation = dataOuter[0].affiliation.S
      var params = {
        TableName: "friends",
        KeyConditionExpression: "#userid = :userid",
        ExpressionAttributeNames: {
          "#userid": "userid",
        },
        ExpressionAttributeValues: {
          ":userid": { "S": userid }
        },
        ProjectionExpression: "useridfriend"
      };
      db.query(params, function (err, dataInner) {
        if (err) {
          callback(err, null);
        } else {
          var friends = dataInner.Items;
          var flagTotal = 0;
          var flag = 0
          var results = [];
          for (var i = 0; i < friends.length; i++) {
            var friend = friends[i].useridfriend.S;
            var params = {
              TableName: "friends",
              KeyConditionExpression: "#userid = :userid",
              ExpressionAttributeNames: {
                "#userid": "userid",
              },
              ExpressionAttributeValues: {
                ":userid": { "S": friend }
              },
              ProjectionExpression: "useridfriend"
            };
            db.query(params, function (err, dataInnerest) {
              if (err) {
                callback(err, null);
              } else {
                var friendsOfFriend = dataInnerest.Items;
                flagTotal += friendsOfFriend.length
                for (var j = 0; j < friendsOfFriend.length; j++) {
                  var friendOfFriend = friendsOfFriend[j].useridfriend.S;
                  myDB_getUserDetails(friendOfFriend, function (err, dataInnerest2) {
                    if (err) {
                      callback(err, null);
                    } else {
                      flag += 1
                      if (dataInnerest2[0].userid.S != userid && !results.includes(dataInnerest2[0].userid.S)) {
                        if (limit == false) {
                          results.push(dataInnerest2[0].userid.S);
                        } else if (dataInnerest2[0].affiliation.S == userAffiliation) {
                          results.push(dataInnerest2[0].userid.S);
                        }
                      }
                      if (flag == flagTotal) {
                        callback(null, results);
                      }
                    }
                  });
                }
              }
            });
          }
        }
      });
    }
  })
}


var myDB_getFriendsWithAffiliation = function (userid, affiliation, callback) {
  myDB_getFriends(userid, function (err, dataOuter) {
    if (err) {
      callback(err, null);
    } else {
      var friends = dataOuter;
      var flag = 0
      var results = [];
      for (var i = 0; i < friends.length; i++) {
        var friend = friends[i].useridfriend.S;
        myDB_getUserDetailsSparse(friend, function (err, dataInner) {
          if (err) {
            callback(err, null);
          } else {
            flag += 1;
            if (dataInner[0].affiliation.S == affiliation) {
              results.push(dataInner[0]);
            }
            if (flag == friends.length) {
              callback(null, results);
            }
          }
        });
      }
    }
  });
}


// function to add record of user post to user-posts table
var myDB_addUserPost = function (userid, destination, tsunix, callback) {
  var params = {
    TableName: "user-posts",
    Item: {
      userid: { S: userid },
      destination: { S: destination },
      postid: { S: userid + "-" + tsunix },
      tsunix: { N: tsunix }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
      console.log("add post from db success, postid =" + userid + "-" + tsunix);
    }
  });
}

// function to delete a postid from user-posts table
var myDB_deleteUserPost = function (userid, tsunix, callback) {
  var params = {
    TableName: "user-posts",
    Key: {
      userid: { S: userid },
      tsunix: { N: tsunix }
    }
  };
  db.deleteItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//function to get top n most recent posts from a given userid with a tsunix less than a given tsunix value
var myDB_getUserPosts = function (userid, ts, n, callback) {
  var params = {
    TableName: "user-posts",
    KeyConditionExpression: "#userid = :userid and #tsunix < :tsunix",
    ExpressionAttributeNames: {
      "#userid": "userid",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: false,
    ProjectionExpression: "postid, tsunix",
    Limit: n
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

//wrapper function to delete a post from the posts table and then the same post in the user-posts table
var myDB_deletePostWrapper = function (postid, callback) {
  myDB_deletePost(postid, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      var userid = postid.split("-")[0]
      var tsunix = postid.split("-")[1]
      myDB_deleteUserPost(userid, tsunix, function (err, data) {
        if (err) {
          callback(err, null);
        } else {
          callback(null, "Success");
        }
      });
    }
  });
}

//Function that calls getFriends on a given userid to get all their friends
//And then calls getUserposts to get all their friend's posts with a tsunix less than
//a given tsunix and returns top n most recent posts
var myDB_getMainPosts = function (userid, ts, n, callback) {
  var params = {
    TableName: "friends",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "useridfriend"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      var friends = data.Items;
      friends.push(userid);
      var flag = 0
      var done = 0
      var posts = [];
      for (var i = 0; i < friends.length; i++) {
        try {
          var friend = friends[i].useridfriend.S;
        } catch (error) {
          var friend = friends[i]
        }
        myDB_getUserPosts(friend, ts, n, function (err, data) {
          if (err) {
            callback(err, null);
          } else {
            posts = posts.concat(data);
            flag += 1
            if (flag == friends.length && done == 0) {
              done = 1
              //go through posts and return only top 10 most recent ones
              posts.sort(function (a, b) {
                return b.tsunix.N - a.tsunix.N;
              });
              callback(null, posts.slice(0, n));
            }
          }
        });
      }
    }
  });
}

//function to get posts destined for a given wall
var myDB_getWallPosts = function (destination, ts, n, callback) {
  var params = {
    TableName: "user-posts",
    IndexName: "destination-tsunix-index",
    KeyConditionExpression: "#destination = :destination and #tsunix < :tsunix",
    ExpressionAttributeNames: {
      "#destination": "destination",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":destination": { "S": destination },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: false,
    ProjectionExpression: "postid, tsunix",
    Limit: n
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

//function to get top n least recent posts from a given userid with a tsunix more than a given tsunix value
var myDB_getUserPostsForward = function (userid, ts, n, callback) {
  var params = {
    TableName: "user-posts",
    KeyConditionExpression: "#userid = :userid and #tsunix > :tsunix",
    ExpressionAttributeNames: {
      "#userid": "userid",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: true,
    ProjectionExpression: "postid, tsunix",
    Limit: n
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

//Function that calls getFriends on a given userid to get all their friends
//And then calls getUserposts to get all their friend's posts with a tsunix more than
//a given tsunix and returns top n least recent posts
var myDB_getMainPostsForward = function (userid, ts, n, callback) {
  var params = {
    TableName: "friends",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "useridfriend"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      var friends = data.Items;
      friends.push(userid);
      var flag = 0
      var done = 0
      var posts = [];
      for (var i = 0; i < friends.length; i++) {
        try {
          var friend = friends[i].useridfriend.S;
        } catch (error) {
          var friend = friends[i]
        }
        myDB_getUserPostsForward(friend, ts, n, function (err, data) {
          if (err) {
            callback(err, null);
          } else {
            posts = posts.concat(data);
            flag += 1
            if (flag == friends.length && done == 0) {
              done = 1
              //go through posts and return only top 10 least recent ones
              posts.sort(function (a, b) {
                return a.tsunix.N - b.tsunix.N;
              });
              callback(null, posts.slice(0, n));
            }
          }
        });
      }
    }
  });
}

//function to get posts destined for a given wall after a given tsunix, returns top n least recent
var myDB_getWallPostsForward = function (destination, ts, n, callback) {
  var params = {
    TableName: "user-posts",
    IndexName: "destination-tsunix-index",
    KeyConditionExpression: "#destination = :destination and #tsunix > :tsunix",
    ExpressionAttributeNames: {
      "#destination": "destination",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":destination": { "S": destination },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: true,
    ProjectionExpression: "postid, tsunix",
    Limit: n
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}


// function to create a new post in posts table
var myDB_addPost = function (creator, tsunix, content, callback) {
  var params = {
    TableName: "posts",
    Item: {
      postid: { S: creator + "-" + tsunix },
      creator: { S: creator },
      tsunix: { N: tsunix },
      content: { S: content }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

// function to delete a post from the posts table
var myDB_deletePost = function (postid, callback) {
  var params = {
    TableName: "posts",
    Key: {
      postid: { S: postid }
    }
  };
  db.deleteItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//function to get details of a post from the posts table given a postid
var myDB_getPost = function (postid, callback) {
  var params = {
    TableName: "posts",
    Key: {
      postid: { S: postid }
    }
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Item);
    }
  });
}

// function to add a post record to user-posts by calling addUserPost and
// calling addPost to add the corresponding post to posts table
var my_DBcreatePost = function (userid, destination, tsunix, content, callback) {
  myDB_addUserPost(userid, destination, tsunix, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      myDB_addPost(userid, tsunix, content, function (err, data) {
        if (err) {
          callback(err, null);
        } else {
          callback(null, "Success");
        }
      });
    }
  });
}

// function to add a comment to the comments table given postid, creator, content, tsunix
var myDB_addComment = function (postid, creator, content, tsunix, callback) {
  var params = {
    TableName: "comments",
    Item: {
      postid: { S: postid },
      commentid: { S: creator + '-' + tsunix },
      creator: { S: creator },
      tsunix: { N: tsunix },
      content: { S: content }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

// function to delete a comment from the comments table given postid and commentid
var myDB_deleteComment = function (commentid, callback) {
  var params = {
    TableName: "comments",
    Key: {
      commentid: { S: commentid }
    }
  };
  db.deleteItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

// function to get n most recent comments from comments table given postid
// with a tsunix less than a given tsunix value
var myDB_getComments = function (postid, ts, n, callback) {
  var params = {
    TableName: "comments",
    IndexName: "postid-tsunix-index",
    KeyConditionExpression: "#postid = :postid and #tsunix < :tsunix",
    ExpressionAttributeNames: {
      "#postid": "postid",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":postid": { "S": postid },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: false,
    ProjectionExpression: "commentid, content, creator, tsunix",
    Limit: n
  };
  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

//Function to add a new chatid to chats
var myDB_addChat = function (chatid, tsunix, type, name, callback) {
  if (chatid == null) {
    chatid = makeRandomId();
  }
  var params = {
    TableName: "chats",
    Item: {
      chatid: { S: chatid },
      tsunix: { N: tsunix },
      type: { N: type },
      name: { S: name }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, chatid);
    }
  });
}

//function to get the name of a given chatid
var myDB_getChatName = function (chatid, callback) {
  var params = {
    TableName: "chats",
    KeyConditionExpression: "chatid = :chatid",
    ExpressionAttributeValues: {
      ":chatid": {
        S: chatid
      }
    },
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items[0].name);
    }
  });
}

//function to change the name of a given chatid
var myDB_changeChatName = function (chatid, name, callback) {
  var params = {
    TableName: "chats",
    Key: {
      chatid: { S: chatid }
    },
    UpdateExpression: "set #name = :name",
    ExpressionAttributeValues: {
      ":name": {
        S: name
      }
    },
    ExpressionAttributeNames: {
      "#name": "name",
    },
    ReturnValues: "UPDATED_NEW"
  };

  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data);
    }
  });
}

//function to change the picture of a given chatid
var myDB_changeChatPicture = function (chatid, picture, callback) {
  var params = {
    TableName: "chats",
    Key: {
      chatid: { S: chatid }
    },
    UpdateExpression: "set picture = :picture",
    ExpressionAttributeValues: {
      ":picture": {
        S: picture
      }
    },
    ReturnValues: "UPDATED_NEW"
  };

  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data);
    }
  });
}

//function to get the picture of a given chatid
var myDB_getChatPicture = function (chatid, callback) {
  var params = {
    TableName: "chats",
    KeyConditionExpression: "chatid = :chatid",
    ExpressionAttributeValues: {
      ":chatid": {
        S: chatid
      }
    },
    ProjectionExpression: "picture"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items[0].picture);
    }
  });
}


// function to remove chatid from chats table
var myDB_removeChat = function (chatid, callback) {
  var params = {
    TableName: "chats",
    Key: {
      chatid: { S: chatid }
    }
  };
  db.deleteItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}



// function to get most mostrecent of a given chatid
var myDB_getMostRecentChat = function (chatid, callback) {
  console.log("This is the chatid" + chatid);
  var params = {
    TableName: "chats",
    KeyConditionExpression: "#chatid = :chatid",
    ExpressionAttributeNames: {
      "#chatid": "chatid",
    },
    ExpressionAttributeValues: {
      ":chatid": { S: chatid }
    }
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items[0]);
    }
  });
}

//function to update the latestmessage tsunix of a given chatid
var myDB_updateMostRecentChat = function (chatid, messageid, tsunix, callback) {
  var params = {
    TableName: "chats",
    Key: {
      chatid: { S: chatid }
    },
    UpdateExpression: "SET tsunix = :tsunix, messageid = :messageid",
    ExpressionAttributeValues: {
      ":tsunix": { "N": tsunix },
      ":messageid": {"S": messageid}
    },
    ReturnValues: "UPDATED_NEW"
  };

  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}


// Function to add new chatmessage to chatmessages table
var myDB_addChatMessage = function (chatid, creator, content, tsunix, callback) {
  var randomId = makeRandomId()
  var params = {
    TableName: "chat-messages",
    Item: {
      chatid: { S: chatid },
      creator: { S: creator },
      messageid: { S: randomId },
      tsunix: { N: tsunix },
      content: { S: content }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, randomId);
    }
  });
}

// wrapper function that calls both addChatMessage and updateMostRecentChat to add
// an entry to both given a new chat message
var myDB_createChatMessage = function (chatid, creator, content, tsunix, callback) {
  myDB_addChatMessage(chatid, creator, content, tsunix, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      myDB_updateMostRecentChat(chatid, data, tsunix, function (err, dataInner) {
        if (err) {
          callback(err, null);
        } else {
          console.log("Successfully add message");
          callback(null, data);
        }
      });
    }
  });
}

// function to get n sorted most recent chatmessages from chatmessages table given chatid
// with tsunix value less than given tsunix value
var myDB_getChatMessages = function (chatid, ts, n, callback) {
  var params = {
    TableName: "chat-messages",
    IndexName: "chatid-tsunix-index",
    KeyConditionExpression: "#chatid = :chatid and #tsunix < :tsunix",
    ExpressionAttributeNames: {
      "#chatid": "chatid",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":chatid": { "S": chatid },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: false,
    ProjectionExpression: "content, creator, tsunix, messageid",
    Limit: n
  };
  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

// function to get n sorted least recent chatmessages from chatmessages table given chatid
// with tsunix value greater than given tsunix value
var myDB_getChatMessagesForward = function (chatid, ts, n, callback) {
  var params = {
    TableName: "chat-messages",
    IndexName: "chatid-tsunix-index",
    KeyConditionExpression: "#chatid = :chatid and #tsunix > :tsunix",
    ExpressionAttributeNames: {
      "#chatid": "chatid",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":chatid": { "S": chatid },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: true,
    ProjectionExpression: "content, creator, tsunix",
    Limit: n
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

var myDB_getChatMessageById = function(messageid, callback) {
  var params = {
    TableName: "chat-messages",
    KeyConditionExpression: "#messageid = :messageid",
    ExpressionAttributeNames: {
      "#messageid":"messageid"
    },
    ExpressionAttributeValues: {
      ":messageid": {"S": messageid}
    }
  }
  db.query(params, function (err, data){
    if (err) {
      callback(err, null);
    } else {
      console.log(data.Items[0].creator.S);
      myDB_getUserDetailsSparse(data.Items[0].creator.S, function(error, dat) {
        if (error) {
          callback(err, null);
        } else {
          var da = data.Items[0];
          console.log(da);
          console.log(dat);
          da.creatorname = dat[0].username;
          console.log(da);
          callback(null, da);
        }
      })
    }
  });
}

//Function to add a chatid to a given userid's chatids string set in the users table
//Creates a new string set if none previous exists
var myDB_addChatIdToUser = function (userid, chatid, callback) {
  var params = {
    TableName: "users",
    Key: {
      userid: { S: userid }
    },
    UpdateExpression: "ADD chatids :chatid",
    ExpressionAttributeValues: {
      ":chatid": { SS: [chatid] }
    },
    ReturnValues: "UPDATED_NEW"
  };
  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//Function to remove a chatid from a given userid's chatids string set in the users table

var myDB_removeChatIdFromUser = function (userid, chatid, callback) {
  var params = {
    TableName: "users",
    Key: {
      userid: { S: userid }
    },
    UpdateExpression: "DELETE chatids :chatid",
    ExpressionAttributeValues: {
      ":chatid": { SS: [chatid] }
    },
    ReturnValues: "UPDATED_NEW"
  };
  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//Function to get all chatids for a given userid
var myDB_getUserChatIds = function (userid, callback) {
  var params = {
    TableName: "users",
    Key: {
      userid: { S: userid }
    },
    ProjectionExpression: "chatids"
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      if (typeof data.Item.chatids != "undefined") {
        callback(null, data.Item.chatids.SS);
      }
    }
  });
}

//Function to add a userid to a given chatid's userid string set in the chats table
var myDB_addUserIdToChat = function (chatid, userid, callback) {
  var params = {
    TableName: "chats",
    Key: {
      chatid: { S: chatid }
    },
    UpdateExpression: "ADD userids :userid",
    ExpressionAttributeValues: {
      ":userid": { SS: [userid] }
    },
    ReturnValues: "UPDATED_NEW"
  };
  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//Function to remove a userid from a given chatid's userid string set in the chats table
var myDB_removeUserIdFromChat = function (chatid, userid, callback) {
  var params = {
    TableName: "chats",
    Key: {
      chatid: { S: chatid }
    },
    UpdateExpression: "DELETE userids :userid",
    ExpressionAttributeValues: {
      ":userid": { SS: [userid] }
    },
    ReturnValues: "UPDATED_NEW"
  };
  db.updateItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//Function to get all userids for a given chatid
var myDB_getChatUserIds = function (chatid, callback) {
  if (chatid == null) {
    chatid = makeRandomId()
  }
  console.log("Getting chat" + chatid);
  var params = {
    TableName: "chats",
    Key: {
      chatid: { S: chatid }
    },
    ProjectionExpression: "userids"
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      console.log(data.Item);
      callback(null, data.Item.userids.SS);
    }
  });
}

//wrapper function to add a given userid to a chatid
//Calls both addUserIdToChat and addChatIdToUser
var myDB_addUserToChat = function (userid, chatid, callback) {
  myDB_addUserIdToChat(chatid, userid, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      myDB_addChatIdToUser(userid, chatid, function (err, data) {
        if (err) {
          callback(err, null);
        } else {
          callback(null, "Success");
        }
      });
    }
  });
}

//wrapper function to remove a given userid from a chatid
//Calls both removeUserIdFromChat and removeChatIdFromUser
var myDB_removeUserFromChat = function (userid, chatid, callback) {
  myDB_removeUserIdFromChat(chatid, userid, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      myDB_removeChatIdFromUser(userid, chatid, function (err, data) {
        if (err) {
          callback(err, null);
        } else {
          callback(null, "Success");
        }
      });
    }
  });
}

// function to add newsid, category, content, tsunix to news table
var myDB_addNews = function (newsid, category, content, tsunix, callback) {
  var params = {
    TableName: "news",
    Item: {
      newsid: { S: newsid },
      category: { S: category },
      content: { S: content },
      tsunix: { N: tsunix }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

// function to get n most recent news from news table with a given category and
// a tsunix less than a given tsunix value
var myDB_getNews = function (userid, category, ts, n, callback) {
  var params = {
    TableName: "news",
    IndexName: 'category-tsunix-index',
    KeyConditionExpression: "#category = :category and #tsunix < :tsunix",
    ExpressionAttributeNames: {
      "#category": "category",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":category": { "S": category },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: false,
    Limit: n
  };
  var flag = 0
  var weights = {}
  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
	  for (var i = 0; i < data.Items.length; i++){
		myDB_getNewsWeight(userid, data.Items[i].newsid.S, function (err, weight) {
			if (err){
				callback(err, null)
			} else {
				flag += 1
				weights[weight.newsid.S] = weight.weights.N
				if (flag == data.Items.length) {
					for (var i = 0; i < data.Items.length; i++){
						data.Items[i].weight = weights[data.Items[i].newsid.S]
					}
					callback(null, data.Items)
				}
			}
		})
	  }
    }
  });
}

// function to get content and category of given newsid
var myDB_getNewsContent = function (newsid, callback) {
  var params = {
    TableName: "news",
    KeyConditionExpression: "#newsid = :newsid",
    ExpressionAttributeNames: {
      "#newsid": "newsid"
    },
    ExpressionAttributeValues: {
      ":newsid": { "S": newsid }
    },
  };
  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items[0]);
    }
  });
}

var myDB_searchNews = function (searchTerm, userid, callback) {
  // Store ids that will be looked up 
  var ids = []
  var idCounts = new Object() //to be used in ranking later
  var idPromises = [] //store promises for ids

  //Split request into array of queries
  let queries = searchTerm.trim().split(" ");

  //Process queries by lowercasing, removing nonalpha, and stemming
  for (var i = 0; i < queries.length; i++) {
    let term = queries[i];
    term = term.toLowerCase().replace(/[^a-z]/g, '');
    queries[i] = stemmer(term);
  };

  //Loop through queries and add Ids to resultIds
  for (i = 0; i < queries.length; i++) {
    var params = {
      KeyConditionExpression: 'keyword = :keyword',
      ExpressionAttributeValues: {
        ':keyword': { S: queries[i] }
      },
      TableName: 'invertedNews'
    };
    idPromises.push(db.query(params).promise());
  }

  //Create array of promises
  var promises = []
  var flag = 0
  var weights = {}

  Promise.all(idPromises).then(
    //Code chunk to get ids from keyword query
    promiseArraySuccess => {
      promiseArraySuccess.forEach(
        (successResult) => successResult.Items.forEach((item) => ids.push(item.inxid.N))
      )
      for (i = 0; i < ids.length; i++) {
        idCounts[ids[i]] = idCounts[ids[i]] ? idCounts[ids[i]] + 1 : 1;
      }
      ids = [...new Set(ids)]
    },
    promisesError => {
      callback(promisesError, null)
    }

  ).then(
    success => { //code chunk to generate promises for queries
      for (i = 0; i < ids.length; i++) {
        var params = {
          KeyConditionExpression: 'newsid = :newsid',
          ExpressionAttributeValues: {
            ':newsid': { S: String(ids[i]) }
          },
          TableName: 'news'
        };
        promises.push(db.query(params).promise());
      }
    }).then(success => { //code chunk to create request object from queries
      Promise.all(promises).then(
        SDArray => {
          var results = []
          for (i = 0; i < SDArray.length; i++) {
            if (SDArray[i].Items.length == 1) {
              var result = new Object();
              var newsid = SDArray[i].Items[0]['newsid'].S
              result['queryMatches'] = idCounts[newsid];
              result['tsunix'] = SDArray[i].Items[0]['tsunix'].N;
              result['authors'] = SDArray[i].Items[0]['authors'].S;
              result['category'] = SDArray[i].Items[0]['category'].S;
              result['headline'] = SDArray[i].Items[0]['headline'].S;
              result['link'] = SDArray[i].Items[0]['link'].S;
              result['newsid'] = newsid
              result['short_description'] = SDArray[i].Items[0]['short_description'].S;
              results.push(result);
            }
          }
          for (var i = 0; i < results.length; i++){
			myDB_getNewsWeight(userid, results[i].newsid, function (err, weight) {
				if (err){
					callback(err, null)
				} else {
					flag += 1
					weights[weight.newsid.S] = weight.weights.N
					if (flag == results.length) {
						for (var i = 0; i < results.length; i++){
							results[i].weight = weights[results[i].newsid]
						}
						results.sort(
				            function (a, b) {
				              return parseInt(b.queryMatches) - parseInt(a.queryMatches);
				        });
						results.sort(
				            function (a, b) {
				              return parseFloat(b.weight) - parseFloat(a.weight);
				        });
				        callback(null, results)
					}
				}
			})
		  }
        },
        errorDataArray => {
          callback(errorDataArray, null);
        });
    });
}

//Function that calls getUserDetails on a given userid to get all their interests
//And then calls getNews to get all their friend's posts with a tsunix less than
//a given tsunix and returns top n most recent posts
var myDB_getNewsfeed = function (userid, ts, n, callback) {
  var interests = [];
  var params = {
    TableName: "users",
    Key: {
      "userid": {
        S: userid
      }
    },
    ProjectionExpression: "interests"
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      interests = data.Item.interests.SS;
      var news = [];
      var flag = 0
	  console.log(interests)
      for (var i = 0; i < interests.length; i++) {
        myDB_getNews(userid, interests[i], ts, n, function (err, data) {
          if (err) {
            callback(err, null);
          } else {
            flag += 1
			console.log('blap')
            news = news.concat(data);
            if (flag == interests.length) {
              //return top n news with highest tsunix
              news.sort(function (a, b) {
                return b.tsunix.N - a.tsunix.N;
              });
			  news.sort(function (a, b) {
                return b.weight - a.weight;
              });
              callback(null, news.slice(0, n));
            }
          }
        });
      }
    }
  });
}

// function to get n most recent notifications from notifications table given userid, type, status, tsunix, and limit n.
// Uses secondary index if type is null
// Uses tertiary index if passed status and type
var myDB_getNotifications = function (userid, type, status, ts, n, callback) {
  var statusUseridKey = status + '_' + userid;
  var params = {
    TableName: "notifications",
    IndexName: "status_userid-tsunix-index",
    KeyConditionExpression: "#status_userid = :statusUseridKey and #tsunix < :tsunix",
    ExpressionAttributeNames: {
      "#status_userid": "status_userid",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":statusUseridKey": { "S": statusUseridKey },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: false,
    Limit: n
  };
  if (type != null) {
    var statusTypeUseridKey = status + '_' + type + '_' + userid;
    params.IndexName = "status_type_userid-tsunix-index";
    params.KeyConditionExpression = "#status_type_userid = :statusTypeUseridKey and #tsunix < :tsunix";
    params.ExpressionAttributeNames = {
      "#status_type_userid": "status_type_userid",
      "#tsunix": "tsunix"
    }
    params.ExpressionAttributeValues = {
      ":statusTypeUseridKey": { "S": statusTypeUseridKey },
      ":tsunix": { "N": ts }
    }
  }
  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

//reverse version of getNotifications
var myDB_getNotificationsForward = function (userid, type, status, ts, n, callback) {
  var statusUseridKey = status + '_' + userid;
  var params = {
    TableName: "notifications",
    IndexName: "status_userid-tsunix-index",
    KeyConditionExpression: "#status_userid = :statusUseridKey and #tsunix >= :tsunix",
    ExpressionAttributeNames: {
      "#status_userid": "status_userid",
      "#tsunix": "tsunix"
    },
    ExpressionAttributeValues: {
      ":statusUseridKey": { "S": statusUseridKey },
      ":tsunix": { "N": ts }
    },
    ScanIndexForward: true,
    Limit: n
  };
  if (type != null) {
    var statusTypeUseridKey = status + '_' + type + '_' + userid;
    params.IndexName = "status_type_userid-tsunix-index";
    params.KeyConditionExpression = "#status_type_userid = :statusTypeUseridKey and #tsunix < :tsunix";
    params.ExpressionAttributeNames = {
      "#status_type_userid": "status_type_userid",
      "#tsunix": "tsunix"
    }
    params.ExpressionAttributeValues = {
      ":statusTypeUseridKey": { "S": statusTypeUseridKey },
      ":tsunix": { "N": ts }
    }
  }
  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items);
    }
  });
}

// function to get specific notification from notifications table given notificationid
var myDB_getNotificationId = function (notificationid, callback) {
  var params = {
    TableName: "notifications",
    Key: {
      notificationid: { S: notificationid }
    }
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Item);
    }
  });
}

// function to add a notification to the notifications table given userid, type, status, tsunix, and content
// Userid is receiver id
var myDB_addNotification = function (userid, type, status, tsunix, content, forceid, callback) {
  var statusTypeUseridKey = status + '_' + type + '_' + userid;
  var statusUseridKey = status + '_' + userid;

  if (forceid == null) {
    var randomid = makeRandomId()
  } else {
    var randomid = forceid
  }
  var params = {
    TableName: "notifications",
    Item: {
      notificationid: { S: randomid },
      status: { S: status },
      type: { S: type },
      status_type_userid: { S: statusTypeUseridKey },
      status_userid: { S: statusUseridKey },
      content: { S: content },
      tsunix: { N: tsunix }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, randomid);
    }
  });
}

// function to delete a notification from the notifications table given notificationid
var myDB_deleteNotificationId = function (notificationid, callback) {
  var params = {
    TableName: "notifications",
    Key: {
      notificationid: { S: notificationid }
    }
  };
  db.deleteItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}


// function to change the status of a notification from the notifications table given notificationid and newstatus
// deletes existing notification and puts new notification in with updated status
var myDB_changeNotificationStatus = function (notificationid, newstatus, callback) {
  var params = {
    TableName: "notifications",
    Key: {
      notificationid: { S: notificationid }
    }
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      var type = data.Item.type.S;
      var status = newstatus;
      var content = data.Item.content.S;
      var ts = data.Item.tsunix.N;
      var userid = data.Item.status_userid.S.split('_')[1];
      myDB_deleteNotificationId(notificationid, function (err, data) {
        if (err) {
          callback(err, null);
        } else {
          myDB_addNotification(userid, type, status, ts, content, notificationid, function (err, data) {
            if (err) {
              callback(err, null);
            } else {
              callback(null, "Success");
            }
          });
        }
      });
    }
  });
}

// function to check if pending friend request is present given receiverid and senderid, returns notificationid
var myDB_friendRequestPresent = function (receiverid, senderid, callback) {
  var params = {
    TableName: "pendingFriendRequests",
    Key: {
      receiverid: { S: receiverid },
      senderid: { S: senderid }
    }
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      if (data.Item == undefined) {
        callback(null, null);
      } else {
        callback(null, data.Item.notificationid.S);
      }
    }
  });
}

// function to add a pending friend request to the pendingFriendRequests table given receiverid, senderid, and notificationid
var myDB_friendAddRequest = function (receiverid, senderid, notificationid, callback) {
  var params = {
    TableName: "pendingFriendRequests",
    Item: {
      receiverid: { S: receiverid },
      senderid: { S: senderid },
      notificationid: { S: notificationid }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

// function to delete a pending friend request from the pendingFriendRequests table given receiverid and senderid
var myDB_friendDeleteRequest = function (receiverid, senderid, callback) {
  var params = {
    TableName: "pendingFriendRequests",
    Key: {
      receiverid: { S: receiverid },
      senderid: { S: senderid }
    }
  };
  db.deleteItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

// wrapper function to send friend request, calls friendAddRequest and addNotification
var myDB_sendFriendRequest = function (senderid, receiverid, tsunix, callback) {
  var content = {
    senderid: senderid,
    receiverid: receiverid
  }
  var contentJson = JSON.stringify(content)
  myDB_friendRequestPresent(receiverid, senderid, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      if (data == null) {
        myDB_addNotification(receiverid, "friendRequest", "unseen", tsunix, contentJson, null, function (err, notificationId) {
          if (err) {
            callback(err, null);
          } else {
            myDB_friendAddRequest(receiverid, senderid, notificationId, function (err, data) {
              if (err) {
                callback(err, null);
              } else {
                callback(null, "Success");
              }
            });
          }
        });
      } else {
        callback(err, "Already Sent Friend Request")
      }
    }
  })
}

// wrapper function to delete a friend request from pending and notifications
var myDB_deleteFriendRequest = function (senderid, receiverid, callback) {
  myDB_friendRequestPresent(receiverid, senderid, function (err, notificationId) {
    if (err) {
      callback(err, null);
    } else {
      if (notificationId == null) {
        callback(null, "No pending request");
      } else {
        myDB_friendDeleteRequest(receiverid, senderid, function (err, data) {
          if (err) {
            callback(err, null);
          } else {
            myDB_deleteNotificationId(notificationId, function (err, data) {
              if (err) {
                callback(err, null);
              } else {
                callback(null, "Success");
              }
            });
          }
        });
      }
    }
  });
}

// wrapper function to accept friend request, calls friendDeleteRequest, changes notificationid to addressed
// calls addfriend if action is "accept"
var myDB_processFriendRequest = function (senderid, receiverid, action, callback) {
  var now = String(Math.floor(new Date().getTime() / 1000));
  console.log(now)
  myDB_friendRequestPresent(receiverid, senderid, function (err, notificationId) {
    if (err) {
      callback(err, null);
    } else {
      if (notificationId == null) {
        callback(null, "No pending request");
      } else {
        myDB_changeNotificationStatus(notificationId, "addressed", function (err, data) {
          if (err) {
            callback(err, null);
          } else {
            if (action == "accept") {
              myDB_addFriend(senderid, receiverid, function (err, data) {
                if (err) {
                  callback(err, null);
                } else {
                  myDB_friendDeleteRequest(receiverid, senderid, function (err, data) {
                    if (err) {
                      callback(err, null);
                    } else {
                      myDB_getUserDetailsSparse(receiverid, function (err, receiverData) {
                        if (err) {
                          callback(err, null);
                        } else {
                          myDB_getUserDetailsSparse(senderid, function (err, senderData) {
                            if (err) {
                              callback(err, null);
                            } else {
                              var sender = senderData[0].firstname.S + " " + senderData[0].lastname.S
                              var receiver = receiverData[0].firstname.S + " " + receiverData[0].lastname.S
                              var contentSender = sender + " and " + receiver + " just became friends!"
                              var contentReceiver = sender + " and " + receiver + " just became friends!"
                              my_DBcreatePost(receiverid, receiverid, now, contentReceiver, function (err, data) {
                                if (err) {
                                  callback(err, null);
                                } else {
                                  my_DBcreatePost(senderid, senderid, now, contentSender, function (err, data) {
                                    if (err) {
                                      callback(err, null);
                                    } else {
                                      callback(null, "Success");
                                    }
                                  })
                                }
                              });
                            }
                          });
                        }
                      })
                    }
                  });
                }
              });
            } else {
              myDB_friendDeleteRequest(receiverid, senderid, function (err, data) {
                if (err) {
                  callback(err, null);
                } else {
                  callback(null, "Success");
                }
              });
            }
          }
        });
      }
    }
  });
}

// function to check if pending chat request is present given receiverid and chatid, returns notificationid
var myDB_chatInviteRequestPresent = function (receiverid, chatid, callback) {
  var params = {
    TableName: "pendingChatRequests",
    Key: {
      receiverid: { S: receiverid },
      chatid: { S: chatid }
    }
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      if (data.Item == undefined) {
        callback(null, null);
      } else {
        callback(null, data.Item.notificationid.S);
      }
    }
  });
}

// function to add a pending chat request to the pendingChatRequests table given receiverid and chatid
var myDB_chatInviteAddRequest = function (receiverid, chatid, notificationid, callback) {
  var params = {
    TableName: "pendingChatRequests",
    Item: {
      receiverid: { S: receiverid },
      chatid: { S: chatid },
      notificationid: { S: notificationid }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

// function to delete a pending chat request from the pendingChatRequests table given receiverid and chatid
var myDB_chatInviteDeleteRequest = function (receiverid, chatid, callback) {
  var params = {
    TableName: "pendingChatRequests",
    Key: {
      receiverid: { S: receiverid },
      chatid: { S: chatid }
    }
  };
  db.deleteItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

// wrapper function to add a chat invite request, calls chatInviteAddRequest and addNotification
var myDB_sendChatInviteRequest = function (chatid, senderid, receiverid, tsunix, callback) {
  var content = {
	chatid: chatid,
  senderid: senderid,
	receiverid: receiverid
  }
  var contentJson = JSON.stringify(content)
   myDB_chatInviteRequestPresent(receiverid, chatid, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      if (data == null) {
	  myDB_addNotification(receiverid, "chatInviteRequest", "unseen", tsunix, contentJson, null, function (err, notificationId) {
		    if (err) {
		      callback(err, null);
		    } else {
		      myDB_chatInviteAddRequest(receiverid, chatid, notificationId, function (err, data) {
		        if (err) {
		          callback(err, null);
		        } else {
		          callback(null, "Success");
		        }
		      });
		    }
		  });
	  } else {
		callback(err, "Already Sent Chat Invite Request")
	  }
	}
	})
}

// wrapper function to accept friend request, calls friendDeleteRequest, changes notificationid to addressed
// calls addfriend if action is "accept"
var myDB_processChatInviteRequest = function (chatid, receiverid, action, callback) {
  myDB_chatInviteRequestPresent(receiverid, chatid, function (err, notificationId) {
    if (err) {
      callback(err, null);
    } else {
      if (notificationId == null) {
        callback(null, "No pending request");
      } else {
        myDB_changeNotificationStatus(notificationId, "addressed", function (err, data) {
          if (err) {
            callback(err, null);
          } else {
            if (action == "accept") {
              myDB_addUserToChat(receiverid, chatid, function (err, data) {
                if (err) {
                  callback(err, null);
                } else {
                  myDB_chatInviteDeleteRequest(receiverid, chatid, function (err, data) {
                    if (err) {
                      callback(err, null);
                    } else {
	                  callback(null, receiverid + " successfully joined " + chatid)
                    }
                  });
                }
              });
            } else {
              myDB_chatInviteDeleteRequest(receiverid, chatid, function (err, data) {
                if (err) {
                  callback(err, null);
                } else {
                  callback(null, receiverid + " successfully rejected " + chatid);
                }
              });
            }
          }
        });
      }
    }
  });
}

//function that adds a new entry to the postlikes table given a userid and a postid
var myDB_addPostLike = function (userid, postid, callback) {
  var params = {
    TableName: "postlikes",
    Item: {
      userid: { S: userid },
      postid: { S: postid }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//function that removes a like from the postlikes table given a userid and a postid
var myDB_removePostLike = function (userid, postid, callback) {
  var params = {
    TableName: "postlikes",
    Key: {
      postid: { S: postid },
      userid: { S: userid }
    }
  };
  db.deleteItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//function that gets all ther userids for a given postid in the postlikes table
var myDB_getPostLikes = function (postid, n, callback) {
  var params = {
    TableName: "postlikes",
    KeyConditionExpression: "#postid = :postid",
    ExpressionAttributeNames: {
      "#postid": "postid",
    },
    ExpressionAttributeValues: {
      ":postid": { "S": postid }
    },
    ProjectionExpression: "userid",
    Limit: n
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items.map(function (item) {
        return item.userid.S;
      }));
    }
  });
}

//function that gets all the postids for a given userid in the postlikes table
var myDB_getUserPostLikes = function (userid, callback) {
  var params = {
    TableName: "postlikes",
    IndexName: "userid-index",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "postid"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items.map(function (item) {
        return item.postid.S;
      }));
    }
  });
}

//function that returns the number of likes for a given postid
var myDB_getPostLikesNumber = function (postid, callback) {
  var params = {
    TableName: "postlikes",
    KeyConditionExpression: "#postid = :postid",
    ExpressionAttributeNames: {
      "#postid": "postid",
    },
    ExpressionAttributeValues: {
      ":postid": { "S": postid }
    },
    ProjectionExpression: "userid"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items.length);
    }
  });
}

// function to check if user has liked a given post
var myDB_userPostLikePresent = function (userid, postid, callback) {
  var params = {
    TableName: "postlikes",
    Key: {
      postid: { S: postid },
      userid: { S: userid }
    }
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      if (data.Item == undefined) {
        callback(null, false);
      } else {
        callback(null, true);
      }
    }
  });
}

//function that adds a new entry to the newslikes table given a userid and a newsid
var myDB_addNewsLike = function (userid, newsid, callback) {
  var params = {
    TableName: "newslikes",
    Item: {
      userid: { S: userid },
      newsid: { S: newsid }
    }
  };
  db.putItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//function that removes a like from the newslikes table given a userid and a newsid
var myDB_removeNewsLike = function (userid, newsid, callback) {
  var params = {
    TableName: "newslikes",
    Key: {
      userid: { S: userid },
      newsid: { S: newsid }
    }
  };
  db.deleteItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, "Success");
    }
  });
}

//function that gets all the userids for a given newsid in the newslikes table
var myDB_getNewsLikes = function (newsid, n, callback) {
  var params = {
    TableName: "newslikes",
    KeyConditionExpression: "#newsid = :newsid",
    ExpressionAttributeNames: {
      "#newsid": "newsid",
    },
    ExpressionAttributeValues: {
      ":newsid": { "S": newsid }
    },
    ProjectionExpression: "userid",
    Limit: n
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items.map(function (item) {
        return item.userid.S;
      }));
    }
  });
}

//function that gets all the newsids for a given userid in the newslikes table
var myDB_getUserNewsLikes = function (userid, callback) {
  var params = {
    TableName: "newslikes",
    IndexName: "userid-index",
    KeyConditionExpression: "#userid = :userid",
    ExpressionAttributeNames: {
      "#userid": "userid",
    },
    ExpressionAttributeValues: {
      ":userid": { "S": userid }
    },
    ProjectionExpression: "newsid"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items.map(function (item) {
        return item.newsid.S;
      }));
    }
  });
}

//function that returns the number of likes for a given newsid
var myDB_getNewsLikesNumber = function (newsid, callback) {
  var params = {
    TableName: "newslikes",
    KeyConditionExpression: "#newsid = :newsid",
    ExpressionAttributeNames: {
      "#newsid": "newsid",
    },
    ExpressionAttributeValues: {
      ":newsid": { "S": newsid }
    },
    ProjectionExpression: "userid"
  };

  db.query(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      callback(null, data.Items.length);
    }
  });
}

// function to check if pending chat request is present given receiverid and chatid, returns notificationid
var myDB_userNewsLikePresent = function (userid, newsid, callback) {
  var params = {
    TableName: "newslikes",
    Key: {
      newsid: { S: newsid },
      userid: { S: userid }
    }
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      if (data.Item == undefined) {
        callback(null, false);
      } else {
        callback(null, true);
      }
    }
  });
}

// get news weight for a given userid, newsid
var myDB_getNewsWeight = function (userid, newsid, callback) {
  var params = {
    TableName: "newsweights",
    Key: {
      newsid: { S: newsid },
      userid: { S: userid }
    }
  };
  db.getItem(params, function (err, data) {
    if (err) {
      callback(err, null);
    } else {
      if (data.Item == undefined) {
        callback(null, {newsid: {S: newsid},
						weights: {N: 0}});
      } else {
        callback(null, data.Item);
      }
    }
  });
}

var database = {
  login: myDB_checkPassword,
  getId: myDB_getId,
  createAccount: myDB_createAccount,
  updateInterests: myDB_updateInterests,
  updateEmail: myDB_updateEmail,
  updateAffiliation: myDB_updateAffiliation,
  updatePassword: myDB_updatePassword,
  getUserDetails: myDB_getUserDetails,
  getUserDetailsSparse: myDB_getUserDetailsSparse,
  getUserAffliationName: myDB_getUserAffiliationName,
  updateStatus: myDB_updateStatus,
  getUserStatus: myDB_getStatus,

  addFriend: myDB_addFriend,
  removeFriend: myDB_removeFriend,
  getFriends: myDB_getFriends,
  getFriendsOfFriends: myDB_getFriendsOfFriends,
  getFriendsWithAffiliation: myDB_getFriendsWithAffiliation,

  addUserPost: myDB_addUserPost,
  deleteUserPost: myDB_deleteUserPost,
  getUserPosts: myDB_getUserPosts,
  getMainPosts: myDB_getMainPosts,
  getWallPosts: myDB_getWallPosts,
  getUserPostsForward: myDB_getUserPostsForward,
  getMainPostsForward: myDB_getMainPostsForward,
  getWallPostsForward: myDB_getWallPostsForward,

  addPost: myDB_addPost,
  deletePost: myDB_deletePost,
  createPost: my_DBcreatePost,
  getPost: myDB_getPost,
  deletePostWrapper: myDB_deletePostWrapper,

  addComment: myDB_addComment,
  getComments: myDB_getComments,
  deleteComment: myDB_deleteComment,

  addChat: myDB_addChat,
  removeChat: myDB_removeChat,
  addChatIdToUser: myDB_addChatIdToUser,
  removeChatIdFromUser: myDB_removeChatIdFromUser,
  getUserChatIds: myDB_getUserChatIds,
  changeChatName: myDB_changeChatName,
  changeChatPicture: myDB_changeChatPicture,
  getChatName: myDB_getChatName,
  getChatPicture: myDB_getChatPicture,
  getChatMessageById: myDB_getChatMessageById,

  addUserIdToChat: myDB_addUserIdToChat,
  removeUserIdFromChat: myDB_removeUserIdFromChat,
  getChatUserIds: myDB_getChatUserIds,
  addUserToChat: myDB_addUserToChat,
  removeUserFromChat: myDB_removeUserFromChat,


  getMostRecentChat: myDB_getMostRecentChat,
  updateMostRecentChat: myDB_updateMostRecentChat,
  addChatMessage: myDB_addChatMessage,
  getChatMessages: myDB_getChatMessages,
  getChatMessagesForward: myDB_getChatMessagesForward,
  createChatMessage: myDB_createChatMessage,

  addNews: myDB_addNews,
  getNews: myDB_getNews,
  getNewsContent: myDB_getNewsContent,
  getNewsfeed: myDB_getNewsfeed,
  searchNews: myDB_searchNews,
  getNewsWeight: myDB_getNewsWeight,

  addNotification: myDB_addNotification,
  deleteNotificationId: myDB_deleteNotificationId,
  getNotifications: myDB_getNotifications,
  getNotificationsForward: myDB_getNotificationsForward,
  getNotificationId: myDB_getNotificationId,
  changeNotificationStatus: myDB_changeNotificationStatus,

  friendRequestPresent: myDB_friendRequestPresent,
  friendAddRequest: myDB_friendAddRequest,
  friendDeleteRequest: myDB_friendDeleteRequest,

  sendFriendRequest: myDB_sendFriendRequest,
  deleteFriendRequest: myDB_deleteFriendRequest,
  processFriendRequest: myDB_processFriendRequest,

  chatInviteRequestPresent: myDB_chatInviteRequestPresent,
  chatInviteAddRequest: myDB_chatInviteAddRequest,
  chatInviteDeleteRequest: myDB_chatInviteDeleteRequest,

  sendChatInviteRequest: myDB_sendChatInviteRequest,
  processChatInviteRequest: myDB_processChatInviteRequest,

  addPostLike: myDB_addPostLike,
  removePostLike: myDB_removePostLike,
  getPostLikes: myDB_getPostLikes,
  getPostLikesNumber: myDB_getPostLikesNumber,
  getUserPostLikes: myDB_getUserPostLikes,
  userPostLikePresent: myDB_userPostLikePresent,

  addNewsLike: myDB_addNewsLike,
  removeNewsLike: myDB_removeNewsLike,
  getNewsLikes: myDB_getNewsLikes,
  getNewsLikesNumber: myDB_getNewsLikesNumber,
  getUserNewsLikes: myDB_getUserNewsLikes,
  userNewsLikePresent: myDB_userNewsLikePresent
}

module.exports = database;