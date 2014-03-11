(function HosebirdDriverModel() {
	var self = this, view = this;

	var eb = new vertx.EventBus(window.location.protocol + '//'
			+ window.location.hostname + ':' + window.location.port
			+ '/eventbus');
	view.event_bus = eb;
	window.model = this;
	self.totalEventCount = ko.observable(0);
	self.totalProgressWidth = ko.computed(function () {
		return Math.floor((self.totalEventCount() / 10) % 100);
	}).extend({ throttle: 100 });

	self.active = ko.observableArray([]);
	self.credentials = ko.observable();
	self.feed = ko.observableArray([]);
	self.maxFeedLength = ko.observable(10);

	self.total = ko.computed(function() {
		return self.active().length;
	});
	self.selected = ko.observable(null);

	// set interval
	var tid = setInterval(function() {
		if (!self.selected()) {
			return;
		}
		self.selected().refresh();
		self.selected().lastN(10);
	}, 3000);

	eb.onclose = function() {
		eb = null;
	};
	
	// if we get bored of progress bars...
	self.stopEventUpdates = function () {
		var c = self.active();
		for (var i = 0; i < c.length; i++) {
			var client = c[i];
			eb.unregisterHandler(client.address + ".events", client.handleEvent);
		}
	};

	self.selectClient = function(client) {
		console.log(client);
		self.selected(client);
	};

	self.clearSelected = function() {
		self.selectClient(null);
	}

	self.selectByCred = function(cred) {
		console.log(cred);
		var c = self.active();
		for (var i = 0; i < c.length; i++) {
			if (c[i].name == cred[0]) {
				self.selectClient(c[i]);
				return;
			}
		}
	}

	self.addFeedEvent = function(item) {
		while (self.feed().length >= self.maxFeedLength()) {
			self.feed.pop();
		}
		if (!item.text) {
			console.log(item)
		}
		self.feed.unshift(item);
	};

	function Client(json) {
		var self = this;
		self.name = json.address.replace(/^.*\./, "");
		self.address = json.address;
		self.stats = ko.observableArray([]);
		self.statValues = {};
		self.config = ko.observable();
		self._update(json);
		self.maxLength = 20;
		self.tweetCount = ko.observable(0);
		self.progressWidth = ko.computed(function () {
			return Math.floor((self.tweetCount() / 10) % 100);
		}).extend({ throttle: 100 });

		self.handleEvent = function (m) { 
			self.processEvent(m);
		};
		eb.registerHandler(self.address + ".events", self.handleEvent);
	}
	
	Client.prototype.cycle = function () {
		var self = this;
		eb.send(self.address, {
			command: 'cycle'
		}, function (msg) {
			console.log(msg);
		});
	};
	
	Client.prototype.shutdown = function () {
		var self = this;
		eb.send(self.address, {
			command: 'shutdown'
		}, function (msg) {
			console.log(msg);
		});
	};
	
	
	Client.prototype.processEvent = function(msg) {
		var self = this;
		if (typeof msg === 'number') {
			self.tweetCount(self.tweetCount() + 1);
			view.totalEventCount(view.totalEventCount() + 1);
		} else {
			console.log(msg);
		}		
	};

	Client.prototype._update = function(json) {
		var self = this;
		self.config(json.config);
		for ( var type in json.stats) {
			var val = json.stats[type];
			if (self.statValues[type]) {
				self.statValues[type](val);
				continue;
			}
			var item = {};
			item.type = type;
			item.value = ko.observable(val);
			self.stats.push(item);
			self.statValues[type] = item.value;
		}
		self.stats.sort(function(left, right) {
			return left.type === right.type ? 0 : (left.type < right.type ? -1 : 1)
		});
	};

	Client.prototype.refresh = function() {
		var self = this;
		eb.send(self.address, {
			'command' : 'status'
		}, function(msg) {
			self._update(msg);
		});
	};

	Client.prototype.lastN = function(n) {
		var self = this;
		eb.send(self.address, {
			'command' : 'last_n',
			'n' : n
		}, function(msg) {
			for (var i = 0; i < msg.items.length; i++) {
				var m = msg.items[i];
				if (m.text) {
					// we must use id_str, because JS doesn't handle the int64
					// properly.
					m.url = "https://twitter.com/" + m.user.screen_name
							+ "/status/" + m.id_str;
				}
				view.addFeedEvent(m);
			}
		});
	};

	function Section(name, template) {
		var self = this;
		self.name = name;
		self.template = template;
		self.selected = ko.observable(false);
	}

	self.sections = ko.observableArray([
			new Section('Recent', 'section-recent'),
			new Section('Add', 'section-add') ]);

	self.selectedSection = ko.observable(self.sections()[0]);

	self.selectSection = function(s) {
		self.selectedSection().selected(false);
		self.selectedSection(s);
		self.selectedSection().selected(true);
	}

	self.selectSection(self.sections()[0]);

	self.sniff = function(f) {
		self.selectClient(null);
		self.selectSection(self.sections()[0]);
	}
	
	eb.onopen = function() {
		self.address = "hbbrowser" + "." + makeUUID();
		ko.applyBindings(self);
		eb.registerHandler(self.address + '.stati', function(msg) {
			console.log(msg);
			self.active.push(new Client(msg));
		});
		eb.registerHandler(self.address + ".sniff", function(msg) {
			self.addFeedEvent(msg)
		});

		eb.publish('hbdriver.multicast', {
			command : 'status',
			replyTo : self.address + '.stati'
		});
		eb.send('hbdriver.credentials', {
			command : 'list'
		}, function(msg) {
			console.log(msg);
			self.credentials(msg);
		});

	};

	function makeUUID() {
		return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g,
				function(a, b) {
					return b = Math.random() * 16, (a == "y" ? b & 3 | 8
							: b | 0).toString(16)
				})
	}

	//
	// self.orderSubmitted = ko.observable(false);
	//
	// self.submitOrder = function() {
	//
	// if (!orderReady()) {
	// return;
	// }
	//
	// var orderItems = ko.toJS(self.items);
	// var orderMsg = {
	// action : "save",
	// collection : "orders",
	// document : {
	// username : self.username(),
	// items : orderItems
	// }
	// }
	//
	// eb.send('vertx.mongopersistor', orderMsg, function(reply) {
	// if (reply.status === 'ok') {
	// self.orderSubmitted(true);
	// // Timeout the order confirmation box after 2 seconds
	// // window.setTimeout(function() { self.orderSubmitted(false); },
	// // 2000);
	// } else {
	// console.error('Failed to accept order');
	// }
	// });
	// };
	//
	// self.username = ko.observable('');
	// self.password = ko.observable('');
	// self.loggedIn = ko.observable(false);
	//
	// self.login = function() {
	// if (self.username().trim() != '' && self.password().trim() != '') {
	// eb.login(self.username(), self.password(), function(reply) {
	// if (reply.status === 'ok') {
	// self.loggedIn(true);
	// } else {
	// alert('invalid login');
	// }
	// });
	// }
	// }
	//
	// function Album(json) {
	// var self = this;
	// self._id = json._id;
	// self.genre = json.genre;
	// self.artist = json.artist;
	// self.title = json.title;
	// self.price = json.price;
	// self.formattedPrice = ko.computed(function() {
	// return '$' + self.price.toFixed(2);
	// });
	// }
	//
	// function CartItem(album) {
	// var self = this;
	// self.album = album;
	// self.quantity = ko.observable(1);
	// }
	//	
	//	
})();

function syntaxHighlight(json) {
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        var cls = 'number';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'key';
            } else {
                cls = 'string';
            }
        } else if (/true|false/.test(match)) {
            cls = 'boolean';
        } else if (/null/.test(match)) {
            cls = 'null';
        }
        return '<span class="' + cls + '">' + match + '</span>';
    });
}
