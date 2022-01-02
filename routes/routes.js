const { reject } = require('async');
const { getChatUserIds, getUserDetails } = require('../models/database.js');
var db = require('../models/database.js');
var AWS = require('aws-sdk');
var dyb = new AWS.DynamoDB();

//Login route, redirect to homepage if user logged in
var getLogin = function (req, res) {
	if (req.session.userid == null) {
		res.render('login.ejs', { message: null });
	} else {
		res.redirect('/main');
	}
};

//Signup route, redirect to restaurant if user logged in
var getSignup = function (req, res) {
	var session = req.session
	if (session.username != null) {
		res.redirect('/');
	}
	res.render('signup.ejs', { message: null });
};


//Check login route, performs validation for login and redirects to restaurant if successful
//Saves user name to session
var checkLogin = function (req, res) {
	var username = req.body.username;
	var password = req.body.password;
	if (username == '' || password == '' || username == null || password == null) {
		res.render('login.ejs', { message: "Please enter your username and password" });
	} else {
		db.login(username, password, function (err, data) {
			if (err) {
				res.render('login.ejs', { message: "error reaching database: " + err });
			} else if (data) {
				db.getId(username, function (err, dataOuter) {
					if (err) {
						res.render('login.ejs', { message: "error reaching database: " + err });
					} else {
						db.updateStatus(dataOuter, "Online", function (err, data) {
							if (err) {
								console.log(err);
								res.send(null);
							}
							else {
								req.session.username = username;
								req.session.userid = dataOuter;
								res.redirect('/main');
							}
						});
					}
				})
			} else {
				res.render('login.ejs', { message: "Incorrect password/username" });
			}
		});
	}

};

//Logout route, destroys session and redirects user to login
var getLogout = function (req, res) {
	var userid = req.session.userid;
	db.updateStatus(userid, "Offline", function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		}
		else {
			req.session.destroy();
			res.redirect("/");
		}
	});
};

//Route to create restaurant, takes in username password and fullname and performs validation
//Error handling included if account taken
var createAccount = function (req, res) {
	var username = req.body.username;
	var password = req.body.password;
	var firstname = req.body.firstname;
	var lastname = req.body.lastname;
	var email = req.body.email;
	var affiliation = req.body.affiliation;
	var bdaytemp = req.body.birthday;
	var birthday = String(new Date(bdaytemp).getTime() / 1000);
	var newsinterests = [];
	
	// Create a list of all possible interests
	areas = ["Entertainment", "Politics", "World News", "Sports", "Business", "Science", "Tech", "Style & Beauty", "Style",
		"Comedy", "Women", "Impact", "Arts", "Arts & Culture", "Media", "Queer Voices", "Latino Voices", "Black Voices",
		"Wellness", "Healthy Living", "Parenting", "Taste", "Weddings", "Divorce", "Education", "College", "Parents", "Fifty",
		"Weird News", "Crime", "Green", "Religion", "Home & Living", "Food & Drink", "Worldpost", "Travel"];
	for (var key in req.body) {
		if (req.body.hasOwnProperty(key)) {
			item = req.body[key];
			if (item == "checked") {
				newsinterests.push(key);
			}
		}
	}
	// Use regex to verify the validity of username, firstname, and lastname inputs
	console.log(newsinterests);
	var regexacct = /^[a-zA-Z0-9_\.]+$/;
	var regexname = /^[a-zA-Z_\.]+$/;

	if (username == '' || password == '' || firstname == '' ||
		lastname == '' || email == '' || affiliation == '' ||
		birthday == '' || newsinterests == '') {
		res.render('signup.ejs', { message: "Please fill out all the forms" });
	} else if (!regexacct.test(username) || !regexname.test(firstname) || !regexname.test(lastname)) {
		res.render('signup.ejs', { message: "Please choose appropriate username, first name, and last name, and please use _ to replace blank spaces" });
	} else {
		db.createAccount(username, password, firstname, lastname,
			email, affiliation, birthday, newsinterests, function (err, data) {
				if (err) {
					res.render('signup.ejs', { message: "error reaching database: " + err });
				} else if (data == 'taken') {
					res.render('signup.ejs', { message: "username already taken" });
				} else {
					db.getId(username, function (err, dataOuter) {
						if (err) {
							res.render('signup.ejs', { message: "error reaching database: " + err });
						} else {
							db.updateStatus(data, "Online", function (err, data) {
							if (err) {
								console.log(err);
								res.send(null);
							}
							else {
								req.session.username = username;
								req.session.userid = dataOuter;
								res.redirect('/main');
							}
						});
						}
					})
				}
			});
	}
};


// Account deletion route by calling db. Redirects to logout after db call.
var getDeleteAccount = function (req, res) {
	var user = req.session.userid;
	db.deleteAccount(user);
	res.redirect('/logout');
}

// Load the main page
var getMain = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;

	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	} else {
		res.render('main.ejs', { message: username});
	}
}


/**
 * Method to get the chat list
 */
var getChatList = function (req, res) {
	var user = req.session.userid;
	if (user == null) {
		res.render('chatlist.ejs');
	} else {
		res.render('chatlist.ejs');
	}
}

//Get the most recent chat
var getChatSessionRecent = function (req, res) {
	var userid = req.session.userid;
	var chatid = req.body.chatid;
	db.getMostRecentChat(chatid, function(err, data){
		if (err) {
			console.log(err);
		} else {
			res.send(data);
		}
	})
}

/**
 * Method for getting the chat sessions
 * @param {*} req 
 * @param {*} res 
 */
var getChatSessions = function (req, res) {
	var user = req.session.userid;
	db.getUserChatIds(user, function (err, dat) {
		if (err) {
			console.log("error");
			res.send(null);
		} else {
			var chatlist = dat;
			const promises = chatlist.map(param => {
				return new Promise((resolve, reject) => {
					db.getMostRecentChat(param, function (err, data) {
						if (err) {
							console.log(err);
							reject(err);
						} else {
							resolve(data);
						}
					});
				})
			});
			Promise.all(promises).then(resolves => {
				res.send(JSON.stringify(resolves));
			});
		}
	});
}

//Get all the chat messages in the current session
var getSessionMessages = function (req, res) {
	username = req.session.userid;
	chatid = decodeURIComponent(req.params.chatid);
	tsunix = req.params.tsunix;
	db.getChatMessages(chatid, tsunix, 20, function (err, data) {
		if (err) {
			console.log("Error at getting session messages: " + err);
			res.send(null);
		} else {
			res.send(JSON.stringify(data));
		}
	});

}

//Get all chat users
var getChatUsers = function (req, res) {
	username = req.session.userid;
	chatid = decodeURIComponent(req.params.chatid);
	db.getChatUserIds(chatid, function (err, dat) {
		if (err) {
			console.log(err);
			res.send(null);
		} else {
			var userlist = dat;
			const promises = userlist.map(param => {
				return new Promise((resolve, reject) => {
					db.getUserDetailsSparse(param, function (err, data) {
						if (err) {
							reject(err);
						} else {
							resolve(data);
						}
					});
				});
			});
			Promise.all(promises).then(resolves => {
				resolves = resolves.map(a => {
					return a[0];
				});
				res.send(JSON.stringify(resolves));
			}, reject => {
				res.send(null);
			})
		}
	});
}

//Find all chats according to neighborid
var findChat = function (req, res) {
	var userid = req.session.userid;
	var neighborid = req.params.neighborid;
	db.getUserChatIds(neighborid, function (err, data) {
		if (err) {
			res.send(null);
		} else {
			res.send(data);
		}
	});

}

//Render chat page
var getChat = function (req, res) {
	var userid = req.session.userid;
	if (typeof userid == "undefined") {
		res.redirect('/');
	} else {
		res.render('chat.ejs', { userid: userid });
	}
}

//Add a new chat to the list
var addChat = function (req, res) {
	var userid = req.session.userid;
	var members = req.body.members;
	var tsunix = req.body.tsunix;
	var type = req.body.type;
	db.addChat(null, tsunix, type, 'Newly Created Chat', function (err, dat) {
		if (err) {
			res.send(null);
		} else {
			var chatid = dat;
			var promises = members.map(id => {
				return new Promise((resolve, reject) => {
					db.addUserToChat(id, chatid, function (error, data) {
						if (error) {
							reject("failure");
						} else {
							resolve("success");
						}
					})
				})
			});
			Promise.all(promises).then(resolve => {
				res.send({ chatid: { S: chatid }, userids: { SS: members }, tsunix: { N: tsunix } });
			}, reject => { res.send(null) });
		}
	});

}

//Delete a chat to the list
var deleteChat = function (req, res) {
	var userid = req.session.userid;
	var members = req.body.members;
	var chatid = req.body.chatid;
	db.removeChat(chatid, function (err, dat) {
		if (err) {
			res.send(null);
		} else {
			var promises = members.map(id => {
				return new Promise((resolve, reject) => {
					db.removeUserFromChat(id, chatid, function (error, data) {
						if (error) {
							reject("failure");
						} else {
							resolve("success");
						}
					})
				})
			});
			Promise.all(promises).then(resolve => {
				res.send({ chatid: { S: chatid }, userids: { SS: members }, tsunix: { N: tsunix } });
			}, reject => { res.send(null) });
		}
	});
}

// Change the name of the chat
var changeChatname = function (req, res) {
	var userid = req.session.userid;
	var chatid = req.body.chatid;
	var newChatname = req.body.newChatname;
	db.changeChatName(chatid, newChatname, function(err, data) {
		if (err != null) {
			console.log(err);
			res.send(null);
		} else {
			res.send(data);
		}
	});
}

//Kick a user from a chat
var kickUser = function (req, res) {
	var userid = req.session.userid;
	var userToDelete = req.body.userToDelete;
	var chatid = req.body.room;
	db.removeUserFromChat(userToDelete, chatid, function (error, data) {
		if (error) {
			res.send(null);
		} else {
			res.send(req.body);
		}
	})
}

//Join a chat through invitation
var joinChats = function (req, res) {
	var rendered = req.session.renderMajorityChat;
	if (rendered == "undefined") {
		rendered = true;
		return res.send({ success: false });
	} else {
		return res.send({ success: true });
	}
}

//Invite a friend to join chat
var inviteChat = function (req, res) {
	var senderid = req.session.userid;
	var receiverid = req.body.receiverid;
	var chatid = req.body.chatid;
	var now = String(Math.floor(new Date().getTime() / 1000));
	db.sendChatInviteRequest(chatid, senderid, receiverid, now, function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		} else {
			res.send('success');
		}
	});
}

//Add a message in the chat
var addMessage = function (req, res) {
	var creator = req.body.sender;
	var chatid = req.body.room;
	var content = req.body.text;
	var tsunix = req.body.tsunix;
	db.createChatMessage(chatid, creator, content, tsunix, function (err, data) {
		if (err) {
			res.send({ messageID: null });
		} else {
			res.send({ messageID: data });
		}
	});
}

//Get chat message from a messageid
var getChatMessagesById = function (req, res) {
	var messageids = req.query.messageids;
	if (messageids != null && messageids.length > 0) {
		const promises = messageids.map(msg => {
			return new Promise((resolve, reject) => {
				db.getChatMessageById(msg, function (err, data) {
					if (err) {
						reject(err);
					} else {
						resolve(data)
					}
				})
			})
		});
		Promise.all(promises).then(resolves => {
			console.log(resolves);
			res.send(JSON.stringify(resolves));
		})
	} else {
		res.send([]);
	}
}

//need the verification profile db cal
var changeProfile = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;
	if (userid == null) {
		res.redirect('/');
		return
	} else {
		res.render('profileupdate.ejs', { message: username.concat(userid) })
	}
}

//Post the creation of a post
var postCreatePost = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;
	tsunix = req.body.time;
	content = req.body.content;
	if (userid != null) {
		db.createPost(userid, userid, tsunix, content, function (err, data) {
			if (err) {
				console.log(err);
			} else {
				console.log("postOwnWall success");
			}
		});
	}
}

//Post the creation of a friend post
var postCreateFriendPost = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;
	destination = req.body.friendid;
	tsunix = req.body.time;
	content = req.body.content;
	if (userid != null) {
		db.createPost(userid, destination, tsunix, content, function (err, data) {
			if (err) {
				console.log(err);
			} else {
				console.log("postOwnWall success");
			}
		});
	}
}

//Get all posts on the main page
var getMainPosts = function (req, res) {
	userid = req.session.userid;
	ts = req.body.ts;
	n = req.body.n;
	db.getMainPosts(userid, ts, n, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			var response = [];
			data.forEach(function (item) {
				response.push(item);
			});
			res.send(JSON.stringify(response));
		}
	});
}

//Get all posts on the wall page
var getWallPosts = function (req, res) {
	userid = req.session.userid;
	ts = req.body.ts;
	n = req.body.n;
	db.getWallPosts(userid, ts, n, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			var response = [];
			data.forEach(function (item) {
				response.push(item);
			});
			res.send(JSON.stringify(response));
		}
	});
}

//Get all posts on the main page after a timestamp
var getMainPostsForward = function (req, res) {
	userid = req.session.userid;
	ts = req.body.ts;
	n = req.body.n;

	db.getMainPostsForward(userid, ts, n, function (err, data) {
		if (err) {
			console.log("empty");
		} else {
			var response = [];
			data.forEach(function (item) {
				response.unshift(item);
			});
			res.send(JSON.stringify(response));
		}
	});

}

//Get all posts on the wall after a timestamp
var getWallPostsForward = function (req, res) {
	userid = req.session.userid;
	ts = req.body.ts;
	n = req.body.n;

	db.getWallPostsForward(userid, ts, n, function (err, data) {
		if (err) {
			console.log("empty");
		} else {
			var response = [];
			data.forEach(function (item) {
				response.unshift(item);
			});
			res.send(JSON.stringify(response));
		}
	});

}

//Post all comments
var postGetComments = function (req, res) {
	postid = req.body.postid;
	ts = req.body.ts;
	n = req.body.n;
	db.getComments(postid, ts, n, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			var response = [];
			data.forEach(function (item) {
				response.push(item);
			});
			res.send(JSON.stringify(response));
		}
	});
}

//Post all the posts of users
var postGetPost = function (req, res) {
	postid = req.body.postid;
	ts = new Date().getTime() / 1000;
	ts = ts.toString();
	var response = [];
	n = 5;
	db.getPost(postid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			response.push(data);
			db.getPostLikesNumber(postid, function (err, data) {
				if (err) {
					console.log(err);
				} else {
					response.push(data);
					db.getComments(postid, ts, n, function (err, data) {
						if (err) {
							console.log(err);
						} else {
							comments = [];
							data.forEach(function (item) {
								comments.push(item);
							});
							if (data.length == 0) {
								response.push([]);
							} else {
								response.push(comments);
							}
							res.send(JSON.stringify(response));
						}
					});
				}
			});
		}
	});
}

//Post the number of likes to a post
var postGetPostLikesNumber = function (req, res) {
	postid = req.body.postid;
	db.getPostLikesNumber(postid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Post a recently added like to a post
var postAddPostLike = function (req, res) {
	postid = req.body.postid;
	userid = req.session.userid;
	db.addPostLike(userid, postid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			console.log("addPost like success");
		}
	});
}

//Post a recently added comment to a post
var postAddComment = function (req, res) {
	postid = req.body.postid;
	content = req.body.content;
	tsunix = new Date().getTime() / 1000;
	userid = req.session.userid;
	db.addComment(postid, userid, content, tsunix.toString(), function (err, data) {
		if (err) {
			console.log(err);
		} else {
			console.log("add comment success");
		}
	});
}


//Render the news page
var getNews = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	} else {
		res.render('news.ejs', { message: username + userid });
	}
}

//Post all recommended news
var postGetNewsfeed = function (req, res) {
	userid = req.session.userid;
	ts = req.body.ts;
	n = req.body.n;
	db.getNewsfeed(userid, ts, n, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Get contents of a news according to newsid
var postGetNewsContent = function (req, res) {
	newsid = req.body.newsid;
	db.getNewsContent(newsid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Render news search page
var getNewsSearch = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	} else {
		res.render('newsSearch.ejs', { message: username + userid });
	}
}

//Post all matching search results
var postGetNewsSearchResults = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;
	query = req.body.query
	db.searchNews(query, userid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	})
}

//Post the number of likes for a news
var postGetNewsLikes = function (req, res) {
	newsid = req.body.newsid;
	db.getNewsLikesNumber(newsid, 10, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Get the number of likes of a news
var postGetNewsLikesNumber = function (req, res) {
	newsid = req.body.newsid;
	db.getNewsLikesNumber(newsid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Post likes to a news
var postAddNewsLike = function (req, res) {
	newsid = req.body.newsid;
	userid = req.session.userid;
	db.addNewsLike(userid, newsid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Remove likes to a news
var postRemoveNewsLike = function (req, res) {
	newsid = req.body.newsid;
	userid = req.session.userid;
	db.removeNewsLike(userid, newsid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Update news likes after a recent like
var postNewsLikePresent = function (req, res) {
	newsid = req.body.newsid;
	userid = req.session.userid;
	db.userNewsLikePresent(userid, newsid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}


//Render friends' page
var getFriendsPage = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	} else {
		res.render('friends.ejs', { message: username + userid });
	}
}

//Get all users' friends
var getUserFriends = function (req, res) {
	userid = req.session.userid;
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	}
	db.getFriends(userid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Get user information of all friends
var getUserFriendsDetails = function (req, res) {
	userid = req.session.userid;
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	}
	db.getFriends(userid, function (err, friends) {
		if (err) {
			console.log(err);
		} else {
			flag = 0
			details = []
			for (var i = 0; i < friends.length; i++) {
				try {
					var friend = friends[i].useridfriend.S;
				} catch (error) {
					var friend = friends[i]
				}
				db.getUserDetails(friend, function (err, data) {
					if (err) {
						callback(err, null);
					} else {
						details = details.concat(data);
						flag += 1
						if (flag == friends.length) {
							res.send(JSON.stringify(details));
						}
					}
				});
			}
		}
	});
}


//Delete a friend from list
var postFriendDelete = function (req, res) {
	userid = req.session.userid;
	targetid = req.body.targetid
	db.removeFriend(userid, targetid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Get users' full details
var postGetUserDetails = function (req, res) {
	requesterid = req.session.userid;
	userid = req.body.targetUserid;
	if (requesterid == userid) {
		db.getUserDetails(userid, function (err, data) {
			if (err) {
				console.log(err);
			} else {
				res.send(JSON.stringify(data));
			}
		});
	} else {
		db.getFriends(requesterid, function (err, data) {
			if (err) {
				console.log(err);
			} else {
				flag = true
				for (var i = 0; i < data.length; i++) {
					if (data[i].useridfriend.S == userid) {
						flag = false
						db.getUserDetails(userid, function (err, data) {
							if (err) {
								console.log(err);
							} else {
								res.send(JSON.stringify(data));
							}
						});

					}
				}
				if (flag) {
					console.log("Unauthorized access")
					res.send("Unauthorized access")
				}
			}
		});
	}
}

//Get users' partial detail
var postGetUserDetailsSparse = function (req, res) {
	userid = req.body.targetUserid;
	db.getUserDetailsSparse(userid, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Render friend search page
var getFriendsSearchPage = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	} else {
		res.render('friendSearch.ejs', { message: username + userid });
	}
}

//Get matching friends according to user search constraints
var getFriendsSearchBroadLimit = function (req, res) {
	userid = req.session.userid;
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	}
	db.getFriends(userid, function (err, dataOuter) {
		if (err) {
			console.log(err);
		} else {
			db.getFriendsOfFriends(userid, true, function (err, dataInner) {
				if (err) {
					console.log(err);
				} else {
					var dataOuter2 = []
					for (var k = 0; k < dataOuter.length; k++) {
						dataOuter2.push(dataOuter[k].useridfriend.S)
					}
					//Send union of dataInner and dataOuter
					var data = [...new Set([...dataOuter2, ...dataInner])]
					var flag2 = 0
					var results = {}
					var details = []
					var done = false
					for (var l = 0; l < data.length; l++) {
						var friend = data[l]
						db.getUserDetailsSparse(friend, function (err, dataUser) {
							if (err) {
								console.log(err);
							} else {
								flag2 += 1
								details.push(dataUser);
								if (flag2 == data.length && !done) {
									done = true
									results.details = details
									results.friends = dataOuter
									res.send(JSON.stringify(results));
								}
							}
						});
					}
				}
			});
		}
	});
}

//Get all "visible" users on friend request page
var getFriendsSearchBroadNoLimit = function (req, res) {
	userid = req.session.userid;
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	}
	db.getFriendsOfFriends(userid, false, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			res.send(JSON.stringify(data));
		}
	});
}

//Get matching friends according to user search
var postGetFriendsSearchSpecific = function (req, res) {
	userid = req.session.userid;
	targetuser = req.body.username;
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	}
	db.getId(targetuser, function (err, targetid) {
		if (err) {
			console.log(err);
		} else {
			db.getUserDetailsSparse(targetid, function (err, data) {
				if (err) {
					console.log(err);
				} else {
					res.send(JSON.stringify(data));
				}
			});
		}
	});
}

//Post sent friend request
var postSendFriendRequest = function (req, res) {
	userid = req.session.userid
	targetuser = req.body.frienduserid;
	var now = String(Math.floor(new Date().getTime() / 1000));
	db.sendFriendRequest(userid, targetuser, now, function (err, data) {
		if (err) {
			console.log(err)
		} else {
			res.send(JSON.stringify(data))
		}
	})
}


/**
 * tsunix: infinity or not
 * status: seen, unseen, addressed
 * type: cRequest, fRequest
 * 
 * 
 * @param {*} req 
 * @param {*} res 
 */
var getNotification = function (req, res) {
	var userid = req.session.userid;
	var tsunix = req.query.tsunix;
	var status = req.query.status;
	var statuslist;
	var direction = req.query.direction;
	var n = 15;
	statuslist = ['unseen', 'seen', 'addressed'];
	var promises;
	if(direction == 'forward') {
		promises = statuslist.map(st => {
			return new Promise((resolve, reject) => {
				db.getNotificationsForward(userid, null, st, tsunix, n, function (err, data) {
					if (err) {
						console.log(err);
						reject(err);
					} else {
						resolve(data);
					}
				});
			})
		});
	} else {
		promises = statuslist.map(st => {
			return new Promise((resolve, reject) => {
				db.getNotifications(userid, null, st, tsunix, n, function (err, data) {
					if (err) {
						console.log(err);
						reject(err);
					} else {
						resolve(data);
					}
				});
			})
		});
	}
	Promise.all(promises).then(resolves => {
		var returnNotifications = [];
		for (var i = 0; i < resolves.length; i++) {
			returnNotifications = returnNotifications.concat(resolves[i]);
		}
		res.send(returnNotifications);
	});
	

}

//Add a notification to user notification list
var postResNotification = function (req,res) {
	var tsunix = req.body.tsunix;
	var content = req.body.content;
	var status = req.body.status;
	var type = req.body.type;
	var action = req.body.action;
	content.action = action;
	db.addNotification(content.senderid, type, status, tsunix, JSON.stringify(content), null, function(err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		} else {
			res.send(data);
		}
	})
}


/**
 * tsunix
 * type: cRequest or fRequest
 * content: {senderid: xxx, receiverid: xxx}
 * 
 * @param {*} req 
 * @param {*} res 
 */
var postNotification = function (req, res) {
	userid = req.session.userid;
	tsunix = req.body.tsunix;
	type = req.body.type;
	content = req.body.content;
	db.addNotification(userid, type, "unseen", tsunix, content, function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		}
		else {
			res.send("success")
		}
	})
}

//Update notifications
var postUpdateNotification = function (req, res) {
	var userid = req.session.userid;
	var notificationid = req.body.notificationid;
	var type = req.body.type;
	var status = req.body.status;
	var content = req.body.content;
	var action = req.body.action;
	if (action != null) {
		if (type == 'friendRequest') {
			db.processFriendRequest(content.senderid, content.receiverid, action, function (err, data) {
				res.send(data);
			});
		} else {
			db.processChatInviteRequest(content.chatid, content.receiverid, action, function (err, data) {
				res.send(data);
			});
		}
	} else {
		db.changeNotificationStatus(notificationid, status, function (err, data) {
			res.send(data);
		});
	}
}

/*
Update user interests upon request
*/
var postUpdateInterests = function (req, res) {
	userid = req.session.userid;
	interests = req.body.interests;
	db.updateInterests(userid, interests, function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		}
		else {
			tsunix = new Date().getTime();
			content = "I have changed my new interests to "+ interests.toString();
			db.createPost(userid, userid, tsunix.toString(), content, function (err, data) {
				if (err) {
					console.log(err);
				}
			});
			res.send("success");
		}
	});
}

//Change user status according to input
var postUpdateStatus = function (req, res) {
	userid = req.session.userid;
	status = req.body.status;
	db.updateStatus(userid, status, function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		}
		else {
			res.send("success");
		}
	});
}

//Change user status to online
var postUpdateStatusOnline = function (req, res) {
	userid = req.session.userid;
	db.updateStatus(userid, "Online", function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		}
		else {
			res.send("success");
		}
	});
}

//Change user status to offline
var postUpdateStatusOffline = function (req, res) {
	userid = req.session.userid;
	db.updateStatus(userid, "Offline", function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		}
		else {
			res.send("success");
		}
	});
}

//Get user status
var postGetStatus = function (req, res) {
	userid = req.session.userid;
	status = req.body.status;
	db.getUserStatus(userid, status, function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		}
		else {
			res.send(data);
		}
	});
}

//Update user email upon request
var postUpdateEmail = function (req, res) {
	userid = req.session.userid;
	email = req.body.email;

	db.updateEmail(userid, email, function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		}
		else {
			res.send("success");
		}
	});
}

//Update user affilication upon request
var postUpdateAffiliation = function (req, res) {
	userid = req.session.userid;
	affiliation = req.body.affiliation;
	db.updateAffiliation(userid, affiliation, function (err, data) {
		if (err) {
			console.log(err);
			res.send(null);
		}
		else {
			tsunix = new Date().getTime();
			content = "I have changed my affiliation to "+ affiliation.toString();
			db.createPost(userid, userid, tsunix.toString(), content, function (err, data) {
				if (err) {
					console.log(err);
				} else {
					console.log("post update aff success");
				}
			});
			res.send("success");
		}
	});
}

//Update user password upon request
var postUpdatePassword = function (req, res) {
	userid = req.session.userid;
	oldpassword = req.body.oldpassword;
	newpassword = req.body.newpassword;
	db.updatePassword(userid, oldpassword, newpassword, function (err, data) {
		if (err) {
			console.log(err);
			res.send(err);
		}
		else {
			if(data =="Success"){
				res.send("success");
			}else {
				res.send("incorrect password");
			}
				
		}
	});
}

//Direct user to his/her own wall
var getOwnWall = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;

	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	}
	res.render('ownwall.ejs', { message: username + userid });
}

//Get all friends' posts on wall
var getFrWallPosts = function (req, res) {
	userid = req.body.friendid;
	ts = req.body.ts;
	n = req.body.n;
	db.getWallPosts(userid, ts, n, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			var response = [];
			data.forEach(function (item) {
				response.push(item);
			});
			res.send(JSON.stringify(response));
		}
	});
}

//Get all friends' posts on wall after a timestamp
var getFrWallPostsForward = function (req, res) {
	userid = req.body.friendid;
	ts = req.body.ts;
	n = req.body.n;
	db.getWallPostsForward(userid, ts, n, function (err, data) {
		if (err) {
			console.log(err);
		} else {
			var response = [];
			data.forEach(function (item) {
				response.push(item);
			});
			res.send(JSON.stringify(response));
		}
	});
}

//Get information of all users in the array
var getUserDetailsAll = function (req,res) {
	var userids = req.query.ids;
	if ((typeof userids != undefined) && userids.length != 0) {
		const promises = userids.map(uid => {
			return new Promise((resolve, reject) => {
				db.getUserDetailsSparse(uid, function(err, data){
					if (err) {
						console.log(err);
						reject(err);
					} else {
						resolve(data);
					}
				})
			});
		});
		Promise.all(promises).then(resolves => {
			var answer = resolves.map(x => {return x[0]});
			res.send(answer);
		});
	} else {
		res.send([]);
	}
}

//Get all posts on the friend wall
var getFriendWall = function (req, res) {
	username = req.session.username;
	userid = req.session.userid;
	friendname = req.params.username;
	
	//check if userid has a friend with the username
	if (req.session.userid == null || req.session.username == null) {
		res.redirect('/');
	}
	var friendid = null;
	var p = new Promise((resolve,reject)=>{
		db.getFriends(userid, function (err, friends) {
			if (err) {
				console.log(err);
			} else {
				flag = 0;
				details = [];
				for (var i = 0; i < friends.length; i++) {
					try {
						var friend = friends[i].useridfriend.S;
					} catch (error) {
						var friend = friends[i]
					}
					temp =false;
					db.getUserDetails(friend, function (err, data) {
						if (err) {
							callback(err, null);
						} else {
							if(data[0].username.S === friendname){
								friendid=data[0].userid.S;
								resolve(friendid);
							}
						}
					});
				}
			}
		});
		
	});
	p.then((value) => {
  		friendid =  value;
  		// expected output: "Success!"
		if(friendid!= null){
			res.render('friendwall.ejs', {friendid:friendid});
			//this technically shouldn't happen
		} else {
			console.log("redirecting");
			res.redirect('/friends');
		}
	});

}

//Get username from id
var postUsernameFromId = function(req,res){
	postid = req.body.postid;
	if (postid != undefined) {
		userid = postid.split("-")[1];
	} else {
		userid = req.body.targetuserid
	}
	db.getUserDetails(userid, function (err, data) {
						if (err) {
							res.send(err);
						} else {
							results = {
								username: data[0].username.S,
								postid: postid
							}
							res.send(JSON.stringify(results));
						}
	});
}


//Visualizer functions
var getVisualizer = function(req, res) {
	res.render('friendvisualizer.ejs');
}

//Initialize the visualizer by supporting the user's 1st degree friends
var getVisualizerInitial = function(req, res) {
	username = req.session.username;
	userid = req.session.userid;
	var json = {};
	json["id"] = userid;
	json["name"] = username;
	children = [];
	db.getFriends(userid, function(err, dataUser){
		if (err) {
			console.log(err)
		} else {
			friendst = []
			dataUser.forEach(function(item) {
				friendst.push(item.useridfriend.S);
			});
			
			/*Initiate an empty list of promises*/
  			var requests = [];
			
			/* Promises to find the user attributes*/
  			for (var i = 0; i < friendst.length; i++) {
	  			var params = {
    				TableName: "users",
    				KeyConditionExpression: "#userid = :userid",
    				ExpressionAttributeNames: {
      					"#userid": "userid",
	  					"#st" : "status"
    				},
    				ExpressionAttributeValues: {
      					":userid": { "S": friendst[i]}
    				},
    				ProjectionExpression: "#userid, username, affiliation, #st"
  				};
				
	
	  			var prom = dyb.query(params).promise();
	  			requests.push(prom);
  			}
					
			/* Execute the promises */
			Promise.all(requests).then(
				successfulDataArray => {
					for (var j = 0; j < successfulDataArray.length; j++) {
						successfulDataArray[j].Items.forEach(function(item) {
							friend = {};
							friend["children"] = [];
							friend["id"] = item.userid.S;										
							friend["name"] = item.username.S;
							friend["data"] = {};									
							children.push(friend);
						});
					}
					json["children"] = children;
					json["data"] = [];
    				res.send(json);
				},
				errorDataArray => {
					console.log("Query Failed");
				}
			);
		}		
	})
}

//Get all the friends of a 1st degree friend with same affiliation
var getVisualizerUser = function(req, res) {
	userid = req.params.user;
	db.getUserDetailsSparse(userid, function(err, dataUser) {
		if (err) {
			console.log(err)
		} else {
			username = dataUser[0].username.S;
			originalid = req.session.userid;
			db.getUserDetailsSparse(originalid, function(err, dataUser) {
				if (err) {
					console.log(err)
				} else {
					affiliation = dataUser[0].affiliation.S;
					var newFriends = {};
					newFriends["id"] = userid;
					newFriends["name"] = username;
					children = [];
					// Add in all friends
					db.getFriends(userid, function(err, dataUser){
						if (err) {
							console.log(err)
						} else {
							friendst = []
							dataUser.forEach(function(item) {
								friendst.push(item.useridfriend.S);
							});
					
							/*Initiate an empty list of promises*/
  							var requests = [];
	
							/* Promises to find the user attributes*/
  							for (var i = 0; i < friendst.length; i++) {
								var params = {
    								TableName: "users",
    								KeyConditionExpression: "#userid = :userid",
    								ExpressionAttributeNames: {
      									"#userid": "userid",
	  									"#st" : "status"
    								},
    								ExpressionAttributeValues: {
      									":userid": { "S": friendst[i]}
    								},
    								ProjectionExpression: "#userid, username, affiliation, #st"
  								};	
		
	  							var prom = dyb.query(params).promise();
	  							requests.push(prom);
  							}
					
							/* Execute the promises */
							Promise.all(requests).then(
								successfulDataArray => {
									if (userid != req.session.userid) {
										for (var j = 0; j < successfulDataArray.length; j++) {
											successfulDataArray[j].Items.forEach(function(item) {
												if (item.affiliation != null && item.affiliation.S == affiliation) {
													friend = {};
													friend["children"] = [];
													friend["id"] = item.userid.S;										
													friend["name"] = item.username.S;
													friend["data"] = {};									
													children.push(friend);
												}
											});
										}
									} else {
										for (var j = 0; j < successfulDataArray.length; j++) {
											successfulDataArray[j].Items.forEach(function(item) {
												friend = {};
												friend["children"] = [];
												friend["id"] = item.userid.S;										
												friend["name"] = item.username.S;
												friend["data"] = {};									
												children.push(friend);
											});
										}
									}
		
									newFriends["children"] = children;
									newFriends["data"] = [];
    								res.send(newFriends);							
								},
								errorDataArray => {
									console.log("Query Failed");
								}
							);
						}
					})
				}
			})
		}
	})
}

//Get the userid in the current session
var getUser = function(req, res) {
	res.send(req.session.userid);
}


var routes = {
	//main
	get_user: getUser,
	get_main: getMain,
	get_login: getLogin,
	get_logout: getLogout,
	get_deleteaccount: getDeleteAccount,
	get_signup: getSignup,
	get_ownwall:getOwnWall,
	post_checklogin: checkLogin,
	post_createaccount: createAccount,
	//profile update
	post_updateInterests: postUpdateInterests,
	post_updateEmail: postUpdateEmail,
	post_updatePassword: postUpdatePassword,
	post_updateAffiliation: postUpdateAffiliation,
	post_updateStatus: postUpdateStatus,
	post_updateStatusOnline: postUpdateStatusOnline,
	post_updateStatusOffline: postUpdateStatusOffline,
	//chat
	get_chat_list: getChatList,
	get_chat_sessions: getChatSessions,
	get_chat: getChat,
	post_change_chatname: changeChatname,
	post_add_message: addMessage,
	find_chat: findChat,
	get_chat_message_by_id: getChatMessagesById,
	post_add_chat: addChat,
	post_delete_chat: deleteChat,
	post_kick_user_from_chat: kickUser,
	post_join_chats: joinChats,
	get_chat_users: getChatUsers,
	get_chat_session_recent: getChatSessionRecent,
	get_session_messages: getSessionMessages,
	post_invite_chat: inviteChat,
	get_changeProfile: changeProfile,
	//posts
	post_createpost: postCreatePost,
	post_getmainposts: getMainPosts,
	post_getmainpostsforward: getMainPostsForward,
	post_getwallposts:getWallPosts,
	post_getwallpostsforward:getWallPostsForward,
	post_getcomments: postGetComments,
	post_getpost: postGetPost,
	post_postlikesnumber: postGetPostLikesNumber,
	post_addpostlike: postAddPostLike,
	post_addcomment: postAddComment,
	post_usernamefromid:postUsernameFromId,
	//news
	get_news: getNews,
	get_newsSearch: getNewsSearch,
	post_getNewsSearchResults: postGetNewsSearchResults,
	post_getNewsfeed: postGetNewsfeed,
	post_getNewsContent: postGetNewsContent,
	post_getNewsLikes: postGetNewsLikes,
	post_getNewsLikesNumber: postGetNewsLikesNumber,
	post_addNewsLike: postAddNewsLike,
	post_removeNewsLike: postRemoveNewsLike,
	post_newsLikePresent: postNewsLikePresent,
	//friends
	get_friendsPage: getFriendsPage,
	get_userFriends: getUserFriends,
	get_userFriendsDetails: getUserFriendsDetails,
	post_friendDelete: postFriendDelete,
	get_friendsSearchPage: getFriendsSearchPage,
	get_friendsSearchBroadLimit: getFriendsSearchBroadLimit,
	get_friendsSearchBroadNoLimit: getFriendsSearchBroadNoLimit,
	post_getFriendsSearchSpecific: postGetFriendsSearchSpecific,
	post_sendFriendRequest: postSendFriendRequest,
	//friendwall
	get_friendwall:getFriendWall,
	post_createfriendpost:postCreateFriendPost,
	post_getfrwallposts:getFrWallPosts,
	post_getfrwallpostsforward:getFrWallPostsForward,
	//user details
	post_getUserDetails: postGetUserDetails,
	post_getUserDetailsSparse: postGetUserDetailsSparse,
	post_getStatus: postGetStatus,

	get_notification: getNotification,
	post_notification: postNotification,
	post_update_notification: postUpdateNotification,
	post_accept_notification: postResNotification,
	//visualizer
	get_visualizer: getVisualizer,
	get_visualizer_initial: getVisualizerInitial,
	get_visualizer_user: getVisualizerUser,

	get_userdetails_all: getUserDetailsAll,
};

module.exports = routes;
