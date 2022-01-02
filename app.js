/* Some initialization boilerplate. Also, we include the code from
   routes/routes.js, so we can have access to the routes. Note that
   we get back the object that is defined at the end of routes.js,
   and that we use the fields of that object (e.g., routes.get_main)
   to access the routes. */

var express = require('express');
var bodyParser = require('body-parser');
//var morgan = require('morgan');
var routes = require('./routes/routes.js');
var cookieParser = require("cookie-parser");
var serveStatic = require('serve-static');
var path = require('path');
var app = express();

var session = require('express-session');
var app = express();
var http = require('http').Server(app);
var io = require('socket.io')(http);

app.use(express.urlencoded());
app.use(express.static("public"));
app.use(bodyParser.urlencoded({ extended: false }));
//app.use(morgan('combined'));
app.use(serveStatic(path.join(__dirname, 'public')));


//TODO Make cookie secure?
app.use(cookieParser());
app.use(session({
    secret: "secretKey",
    cookie: {secure: false},
    saveUninitialized: false,
    resave: false
}));

// Add the socket server side
io.on("connection", function (socket) {
    socket.on("chat message", obj => {
        io.to(obj.room).emit("chat message", obj);
    });
    socket.on("join room", obj => {
        socket.join(obj.room);
        if (obj.emit == true) {
            io.to(obj.room).emit('join room', obj);
        }
    });
    socket.on("leave room", obj => {
        socket.leave(obj.room);
    });
    // Need to add Database Call
    socket.on("kick user", obj => {
        if (typeof io.sockets.sockets[obj.userToDelete] != "undefined") {
            io.sockets.sockets[obj.userToDelete].leave(obj.room);
        }
        io.to(obj.room).emit("kick user", obj);
    });
    socket.on("delete room", obj => {
        obj.userlist.forEach(id => {
            if (typeof io.sockets.sockets[obj.userToDelete] != "undefined") {
                io.sockets.sockets[obj.userToDelete].leave(obj.room);
            }
        });
        socket.leave(obj.room);
    });
    socket.on("change chatname", obj => {
        io.to(obj.chatid).emit("change chatname", obj);
    })
})


/* Only done routing till create accounts. Need more*/
app.get('/', routes.get_login);
app.get('/logout',routes.get_logout);
app.post('/checklogin', routes.post_checklogin);
app.get('/signup', routes.get_signup);
app.post('/createaccount', routes.post_createaccount);

// profile update
app.get('/userdetails',routes.get_changeProfile);
app.post('/updateinterests',routes.post_updateInterests);
app.post('/updatepassword',routes.post_updatePassword);
app.post('/updateaff',routes.post_updateAffiliation);
app.post('/updateemail',routes.post_updateEmail);
app.post('/updatestatusonline',routes.post_updateStatusOnline);
app.post('/updatestatusonline',routes.post_updateStatusOffline);
app.get('/user', routes.get_user);

// chats
app.get('/chatlist', routes.get_chat_list);
app.get('/getchatsessions', routes.get_chat_sessions);
app.get('/chat/recent', routes.get_chat_session_recent);
app.post('/chat/delete', routes.post_delete_chat);
app.post('/chat/changechatname', routes.post_change_chatname);
app.get('/chat/getchatmessagebyid', routes.get_chat_message_by_id);
app.get('/getchatusers/:chatid', routes.get_chat_users);
app.get('/chat', routes.get_chat);
app.post('/sendchatmessage', routes.post_add_message);
app.post('/addchat', routes.post_add_chat);
app.post('/joinchats', routes.post_join_chats);
app.get('/findchat/:neighborid', routes.find_chat);
app.get('/getchatmessages/:chatid/:tsunix', routes.get_session_messages);
app.get('/main', routes.get_main);
app.post('/chat/kickuser', routes.post_kick_user_from_chat);
//app.get('/getnotification/:tsunix', routes.get_notification);
app.get('/notification/get', routes.get_notification);
app.post('/notification/update', routes.post_update_notification);
app.post('/notification/accept', routes.post_accept_notification);
app.post('/chat/invite', routes.post_invite_chat);
//posts and comments and likes
app.post('/getcomments',routes.post_getcomments);
app.post('/getpostids',routes.post_getmainposts);
app.post('/getpostidsforward',routes.post_getmainpostsforward);
app.post('/getpost',routes.post_getpost);
app.post('/createpost',routes.post_createpost);
app.post('/getpostlikesnumber',routes.post_postlikesnumber);
app.post('/addpostlike',routes.post_addpostlike);
app.post('/addcomment',routes.post_addcomment);
app.post('/getusernamefromid',routes.post_usernamefromid);

app.get('/getuserdetailsall', routes.get_userdetails_all);// Input is an array of userid

// own wall
app.get('/ownwall',routes.get_ownwall);
app.post('/getwallpostids',routes.post_getwallposts);
app.post('/getwallpostidsforward',routes.post_getwallpostsforward);

// friends' wall
app.get('/user/:username',routes.get_friendwall);
app.post('/createfriendpost',routes.post_createfriendpost);
app.post('/getfrwallpostids',routes.post_getfrwallposts);
app.post('/getfrwallpostidsforward',routes.post_getfrwallpostsforward);

// news
app.get('/news', routes.get_news);
app.get('/news/search', routes.get_newsSearch);
app.post('/news/search/query', routes.post_getNewsSearchResults);
app.post('/news/getFeed', routes.post_getNewsfeed);
app.post('/news/getNewsContent', routes.post_getNewsContent);
app.post('/news/likes/get', routes.post_getNewsLikes)
app.post('/news/likes/getNumber', routes.post_getNewsLikesNumber)
app.post('/news/likes/add', routes.post_addNewsLike)
app.post('/news/likes/remove', routes.post_removeNewsLike)
app.post('/news/likes/present', routes.post_newsLikePresent)

// friends
app.get('/friends', routes.get_friendsPage);
app.get('/friends/fetch', routes.get_userFriends);
app.get('/friends/fetchDetails', routes.get_userFriendsDetails);
app.post('/friends/delete', routes.post_friendDelete);
app.post('/user/getDetails', routes.post_getUserDetails);
app.post('/user/getStatus', routes.post_getStatus);
app.post('/user/getDetailsSparse', routes.post_getUserDetailsSparse);
app.get('/friends/search', routes.get_friendsSearchPage);
app.get('/friends/search/fetch', routes.get_friendsSearchBroadLimit);
app.post('/friends/search/specific', routes.post_getFriendsSearchSpecific);

// notifications
app.post('/friends/request', routes.post_sendFriendRequest);

// visualizer
app.get('/visualizer', routes.get_visualizer);
app.get('/visualizer/friendvisualization', routes.get_visualizer_initial);
app.get('/visualizer/getFriends/:user', routes.get_visualizer_user);

/* Run the server */

http.listen(8080);
//http.listen(8080);
console.log('Server running on port 8080. Now open http://localhost:8080/ in your browser!');
