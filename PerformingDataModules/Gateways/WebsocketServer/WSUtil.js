//Utils file for accessing data from the websocket server

var socket = null;
var handler = null;
var loghandler = null;
var connected = null;

//Call this to clean up after yourself
function closeSocket() {
  if (!socket) {
    return;
  }
  socket.close();
}

//callback handler
function handleMessage(data){
	handler(data);
	handleLog("Got Message:"+data);
}

//callback handler
function handleLog(data){
	if(loghandler){
		loghandler(data);
	}
}

//default log handler
function writeToConsole(data){
	console.log(data);
}

//connect the socket (needs to be called - passan optional log handler here (i.e. to get rid of the console logging))
function connect(loghandle,conhandle){
	if(typeof(loghandle)==='undefined'){
		loghandler = writeToConsole;
	}
	else{
		loghandler = loghandle;
	}
  connected = conhandle
	var url = "ws:///localhost/"
  	socket = new WebSocket(url);

//callback functions

socket.onopen = function () {
     handleLog('Opened Socket');
     connected();
 };

socket.onmessage = function (event) {
    handleMessage(event.data);
 };

socket.onerror = function () {
    handleLog('Error');
};

socket.onclose = function (event) {
    var logMessage = 'Closed (';
    if ((arguments.length == 1) && ('CloseEvent' in window) &&
        (event instanceof CloseEvent)) {
      logMessage += 'wasClean = ' + event.wasClean;
      // code and reason are present only for
      // draft-ietf-hybi-thewebsocketprotocol-06 and later
      if ('code' in event) {
        logMessage += ', code = ' + event.code;
      }
      if ('reason' in event) {
        logMessage += ', reason = ' + event.reason;
      }
    } 
    else {
      logMessage += 'CloseEvent is not available';
    }
    handleLog(logMessage + ')');
  };

}

//send message to the server. Requires specific format:
//{Request:"<my request>", Args}

function SendRequest(args,handle){
  if (!socket) {
      alert("Not Connected To Server");
    return;
  }
  handler = handle;
  var json = JSON.stringify(args);
  socket.send(json);  
}

function GetArgumentObject(request){
  args = {}
  args.request = request
  return args
}

