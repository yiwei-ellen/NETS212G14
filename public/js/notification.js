// Functions on this page is similar to notification in chat but use in general pages
var cUserid;

/**
 * Converter to local time
 * @param {*} ts 
 * @returns 
 */
var timeStampConvert = function (ts) {

    //get full timeStamp
    dt = new Date()
    var date = new Date(ts - dt.getTimezoneOffset() * 60 * 1000);
    var day = date.getDate();
    var month = date.getMonth() + 1;
    var year = date.getFullYear();
    var hours = date.getHours();
    var minutes = date.getMinutes();
    if (hours > 12) {
        hours -= 12;
        ampm = "PM";
    } else {
        ampm = "AM";
    }
    minutes = minutes < 10 ? "0" + minutes : minutes;
    hours = hours == 0 ? 12 : hours;
    var time = hours + ":" + minutes + " " + ampm;
    return day + "/" + month + "/" + year + " " + time;
}


/**
 * Add scroll event so that if we scroll notification to the buttom it will lazy pagination
 */
$(document).ready(function () {
    $('.notification-main').scroll(function () {
        var div = $(this);
        if (this.scrollHeight + div.scrollTop() > $(this).height() - 5) {
            getNotifications(true);
        }
    })
})
/**
 * If notification list not have it
 * */

var notificationlist = [];
var latestNotifTSunix = Date.now() + 1000000000;
var earliestNotifTSunix = Date.now() + 1000000000;

// Forward means later. Backward means earlier
var getNotifications = function (getEarlier) {
    return new Promise((resolve, reject) => {
        var msg = {
            tsunix: latestNotifTSunix,
            direction: 'forward',
        }
        if (getEarlier) {
            msg.tsunix = earliestNotifTSunix;
            msg.direction = 'backward';
        }
        $.getJSON('/notification/get', msg, function (elements) {
            // Difference include both id and in general status update.
            elements = elements.map(obj => {
                return { notificationid: obj.notificationid.S, tsunix: obj.tsunix.N, content: JSON.parse(obj.content.S), type: obj.type.S, status: obj.status.S };
            })
            var difference = elements.filter(obj => {
                return !notificationlist.includes(obj);
            });
            var idDifference = difference.filter(obj => {
                return !notificationlist.filter(obj2 => { return obj2.notificationid == obj.notificationid }).length != 0;
            });
            var notIdDifference = difference.filter(obj => {
                return !idDifference.includes(obj);
            });
            // idDifference sort in left to right descending order of tsunix
            idDifference.sort((a, b) => {
                return b.tsunix - a.tsunix;
            });
            var usids = idDifference.map(x => { return x.content.senderid });
            var usids2 = idDifference.map(x => { return x.content.receiverid });
            var usids = usids.concat(usids2);
            usids = [...new Set(usids)];
            // Process the idDifference
            if (usids.length == 0) { // No new notification block
                // Process the notIdDifference -> Update the status in notificationlist
                for (var i = 0; i < notIdDifference.length; i++) {

                    // Find the exact notification and will edit it
                    var obj = notIdDifference[i];
                    var index = notificationlist.indexOf(notificationlist.filter(obj2 => { return obj2.notificationid == obj.notificationid })[0]);
                    notificationlist[index] = obj;
                    updateNotification(obj.content.senderid, obj.notificationid, obj.status, obj.type, getEarlier);
                }

                // Update the look up tsunix
                if (notificationlist.length > 0) {
                    earliestNotifTSunix = notificationlist[notificationlist.length - 1].tsunix;
                    latestNotifTSunix = notificationlist[0].tsunix;
                }
                resolve("success");
            } else {

                // Get user details such as pictures and other information for adding notification
                $.getJSON('/getuserdetailsall', { ids: usids }, function (elements) {
                    for (var i = 0; i < idDifference.length; i++) {
                        var obj = idDifference[i];
                        elements = elements.filter(objt => {return objt != null});
                        var friend = elements.filter(objt => {
                            return (objt.userid.S == obj.content.senderid || objt.userid.S == obj.content.receiverid) &&
                                (objt.userid.S != cUserid)
                        }); // Want to get the user while it should not be the current user

                        // Once we have a friend that is the sender of this notification
                        if (friend.length != 0) {
                            var friendname = friend[0].username.S;
                            var friendpic = friend[0].picture.S;
                            addNotification(obj.content.senderid, friendname, obj.notificationid, obj.content.chatname, 
                                obj.content.chatid, obj.status, obj.type, obj.tsunix, obj.content.action, friendpic, getEarlier);
                        }
                    }
                    // Put idDifference as a part of the notification list
                    if (getEarlier) {
                        notificationlist = notificationlist.concat(idDifference);
                    } else {
                        notificationlist = idDifference.concat(notificationlist);
                    }
                    // Process the notIdDifference -> Update the status in notificationlist
                    for (var i = 0; i < notIdDifference.length; i++) {
                        var obj = notIdDifference[i];
                        var index = notificationlist.indexOf(notificationlist.filter(obj2 => { return obj2.notificationid == obj.notificationid })[0]);
                        notificationlist[index] = obj;
                        updateNotification(obj.content.senderid, obj.notificationid, obj.status, obj.type, getEarlier);
                    }
                    if (notificationlist.length > 0) {
                        earliestNotifTSunix = notificationlist[notificationlist.length - 1].tsunix;
                        latestNotifTSunix = notificationlist[0].tsunix;
                    }
                    resolve("success");
                });
            }
        });
    });

}

/**
 * The helper method will find the notification. It will then return it from the entire notification list
 * @param {*} element 
 * @returns 
 */
var findNotification = function (element) {
    var notificationid = $(element).closest('.notification-block')[0].id;
    var notification = notificationlist.filter(obj => { return obj.notificationid == notificationid });
    if (notification.length == 1) {
        notification = notification[0];
    }
    return notification;
}

/**
 * Update notification ill update the status of the notification
 */
var updateNotification = function (senderid, notificationid, status, type, isEarlier) {
    var notifBlock = $('.notification-block#' + notificationid);
    if (notifBlock.length != 0) {
        if (status != 'unseen') { // If we have seen it
            var notifCircle = notifBlock.find('.unseen-icon')[0];
            if (notifCircle.classList.length == 1) {
                notifCircle.classList.push('seen');
            }
        }
        if (status == 'addressed') { // If we have addressed it
            var notifButtons = notifBlock.find('.notification-button-menu')[0];
            if (notifButtons.classList.length == 1) {
                notifCircle.classList.push('addressed');
            }
        }
    } else {
        alert("Error");
    }
}

/**
 * A helper function for adding the notification block from the template
 * @param {*} senderid 
 * @param {*} username 
 * @param {*} notificationid 
 * @param {*} chatname 
 * @param {*} chatid 
 * @param {*} status 
 * @param {*} type 
 * @param {*} tsunix 
 * @param {*} action 
 * @param {*} picture 
 * @param {*} isEarlier 
 */
var addNotification = function (senderid, username, notificationid, chatname, chatid, status, type, tsunix, action, picture, isEarlier) {
    var newBlockTemplate = "<div class='notification-block' id='!Replace-notif-id!'><div><img src=\"!Replace-picture!\" /></div>"
        + "<div class='notification-content !Replace-Type!'><div class='notification-content-name' id=!Replace-userid!>!Replace-username!</div>"
        + "<div class='notification-content-msg-block' !Replace-with-chat!><div class='notification-content-msg'>!Replace-message!</div>"
        + "<div class='notification-content-msg-date'>!Replace-time!</div></div></div>"
        + "<div class='unseen-icon !Replace-circle!'><i class='fas fa-circle fa-xs'></i></div>"
        + "<div class='break'></div><div class='notification-button-menu !Replace-button!'>"
        + "<div class='notification-button accept-notification'><i class='far fa-check-circle'>Accept</i></div>"
        + "<div class='notification-button reject-notification'><i class='far fa-times-circle'>Reject</i></div></div></div>";

    // Replace ids
    var block = newBlockTemplate.replace('!Replace-userid!', senderid).replace('!Replace-notif-id!', notificationid);

    // Replace time
    block = block.replace('!Replace-time!', timeStampConvert(tsunix));

    // Replace picture if valid
    var picture;
    if (picture != undefined && parseInt(picture) >= 0 && parseInt(picture) <= 14) {
        picture = "../owl" + picture + ".png"
    } else {
        picture = "../owl.png"
    }
    block = block.replace('!Replace-picture!', picture);

    // Replace status
    if (status != 'unseen') {
        block = block.replace('!Replace-circle!', 'seen');
    } else {
        block = block.replace('!Replace-circle!', '');
    }
    if (status != "addressed" && senderid != cUserid) {
        block = block.replace('!Replace-button!', '');
    } else {
        block = block.replace('!Replace-button!', 'addressed');
    }

    // If no action, which means this is not an inverse notification
    if (action == null) {
        if (type == "friendRequest") {
            block = block.replace('!Replace-message!', "Want to be your friend").replace('!Replace-Type!', "friendRequest");
        } else {
            block = block.replace('!Replace-message!', "Invites you to chat").replace('!Replace-Type!', "chatRequest");
            block = block.replace('!Replace-with-chat!', chatid);
        }
    } else {
        if (type == "friendRequest") {
            block = block.replace('!Replace-message!', "Accepted your friend request").replace('!Replace-Type!', "friendRequest");
        } else {
            block = block.replace('!Replace-message!', "Accepted your invitation to chat").replace('!Replace-Type!', "chatRequest");
            block = block.replace('!Replace-with-chat!', chatid);
        }
    }

    // Replace username. Replace last because it prevents bad username from corrupt the replace
    block = block.replace('!Replace-username!', username);

    // Only add to receiver or user if is addressed
    if (((status != "addressed" && senderid != cUserid) || (status == "addressed"))) {
        if (isEarlier) { // Append to first
            $('.notification-main').append(block);
        } else {
            $('.notification-main').prepend(block);
        }

        // Update the notification from unseen to seen when click the block
        $('#' + notificationid).on("click", function (e) {
            var notification = findNotification(this);

            // Make sure we are clicking the block. Whenever the button is clicked this method should not work. Otherwise we have concurrent issue
            if (!$('#' + notification.notificationid).find('.notification-button-menu').get(0).contains(e.target)) {
                if (notification.status == 'unseen') {
                    notification.status = 'seen';
                    $.post('/notification/update', notification, function (data) {
                        if (data != null) {
                            $('#' + notification.notificationid).find('.unseen-icon').addClass('seen');
                        } else {
                            alert("error");
                        }
                    });
                }
            }
        });

        /**
         * Accept button. When clicked will update to notification as well as sending inverse notification to the sender to let them know
         */
        $('#' + notificationid).find('.notification-button.accept-notification').on('click', function (e) {
            var notification = findNotification(this);
            notification.action = "accept";
            notification.status = 'addressed';

            // Update the notification table
            $.post('/notification/update', notification, function (data) {
                if (data != null) {
                    $("#" + notification.notificationid).find('.notification-button-menu').addClass('addressed');
                    $("#" + notification.notificationid).find('.unseen-icon').addClass('seen');
                    var resNotification = notification;
                    if (notification.type == "chatInviteRequest") { // Will join the chat after clicked accept
                        const promise = new Promise((resolve, reject) => {
                            resolve("Success");
                        });
                        promise.then(resolve => {
                            joinChat(cUserid, notification.content.chatid, true);
                        });
                    }
                    resNotification.tsunix = Date.now();

                    // Post an inverse notification
                    $.post('/notification/accept', resNotification, function (data) {
                        if (data != null) {
                            $.post('/chat')
                        }
                    });
                } else {
                    alert('error')
                }
            });
        });

        // Reject button. In this case only notification table will be updated. No message will send back
        $('#' + notificationid).find('.notification-button.reject-notification').on('click', function () {
            var notification = findNotification(this);
            notification.action = "reject";
            notification.status = 'addressed';
            $.post('/notification/update', notification, function (data) {
                if (data != null) {
                    $("#" + notification.notificationid).find('.notification-button-menu').addClass('addressed');
                    $("#" + notification.notificationid).find('.unseen-icon').addClass('seen');
                } else {
                    alert('error')
                }
            });
        });
    }
}

// Call on socket.io to join the room
var joinChat = function (userid, room, emit) {
    socket.emit('join room', { sender: userid, room: room, tsunix: Date.now(), emit: emit });
}

// A function that will be called every 10 seconds
var refreshTime = function () {

    // Prevent concurrent issue
    const promise = getNotifications(false);
    promise.then(resolve => {
        setTimeout(refreshTime, 10000);
    })
    // Reset timer
}

// Main function that will start the notification cycle
$(document).ready(function () {
    while ($('.notification-main')[0].firstChild != null) {
        $('.notification-main')[0].firstChild.remove();
    }
    $.get('/user', function (data) {
        cUserid = data;
        getNotifications(true);
        // When this page is ready, first set timer
        setTimeout(refreshTime, 10000);
    })
});

// Toggle for notification site
$(document).ready(function () {
    $('li.notification-trigger').click(function () {
        $('.notification-site').toggle();
    });
})
