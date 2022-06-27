function isJsonString(str) {
	try {
		JSON.parse(str);
	} catch (e) {
		return false;
	}
	return true;
}

var text = '{"id_evento":100,"json":[{"id":2}, {"life":100}, {"action":"DAMAGE"}, {"value":"50"}, {"kills":0}, {"gear":""}],"timestamp":"3006"}';

console.log(isJsonString(text));
