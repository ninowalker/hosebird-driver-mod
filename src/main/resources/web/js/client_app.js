/*
 * Copyright 2011-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var navigation = new (function() {
	var self = this;
	self.breadCrumbs = ko.observableArray();

	self.push = function(url, text, callback) {
		var me = this;
		self.breadCrumbs.push({
			'url' : url,
			'text' : text,
			'active' : true,
			'activate': function () {
				self.popUntil(url, callback);
			}
		});
	}
	self.pop = function () {
		var c = self.breadCrumbs(),
			prev = c[c.length - 2];
		self.popUntil(prev.url, function () {});
	}
	
	self.popUntil = function(url, callback) {
		var c = self.breadCrumbs();
		while (c[c.length - 1].url != url) {
			self.breadCrumbs.pop();
		}
		callback();
	}

	self.reset = function() {
		self.breadCrumbs.removeAll();
	}
	return self;
})();

(function HosebirdDriverModel() {
	var self = this, view = this;

	var eb = new vertx.EventBus(window.location.protocol + '//'
			+ window.location.hostname + ':' + 8080 + '/eventbus');
	view.event_bus = eb;
	window.model = this;
	self.totalEventCount = ko.observable(0);
	self.totalProgressWidth = ko.computed(function() {
		return Math.floor((self.totalEventCount() / 10) % 100);
	}).extend({
		throttle : 100
	});

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

	self.stopEventUpdates = function() {
		var c = self.active();
		for (var i = 0; i < c.length; i++) {
			var Connection = c[i];
			eb
					.unregisterHandler(Connection.address + ".events",
							Connection.handleEvent);
		}

	};

	self.selectConnection = function(connection) {
		if (self.selected()) {
			navigation.pop();
		}
		self.selected(connection);
		navigation.push("#/" + connection.name, connection.name, active, function () {})
	};

	self.clearSelected = function() {
		self.selectConnection(null);
	}

	self.selectByCred = function(cred) {
		console.log(cred);
		var c = self.active();
		for (var i = 0; i < c.length; i++) {
			if (c[i].name == cred[0]) {
				self.selectConnection(c[i]);
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

	function Connection(json) {
		var self = this;
		self.name = json.address.replace(/^.*\./, "");
		self.address = json.address;
		self.stats = ko.observableArray([]);
		self.statValues = {};
		self.config = ko.observable();
		self._update(json);
		self.maxLength = 20;
		self.tweetCount = ko.observable(0);
		self.progressWidth = ko.computed(function() {
			return Math.floor((self.tweetCount() / 10) % 100);
		}).extend({
			throttle : 100
		});

		self.handleEvent = function(m) {
			self.processEvent(m);
		};
		// can't deal wit hall the events.
		//eb.registerHandler(self.address + ".events", self.handleEvent);
	}

	Connection.prototype.cycle = function() {
		var self = this;
		eb.send(self.address, {
			command : 'cycle'
		}, function(msg) {
			console.log(msg);
		});
	};

	Connection.prototype.shutdown = function() {
		var self = this;
		eb.send(self.address, {
			command : 'shutdown'
		}, function(msg) {
			console.log(msg);
		});
	};

	Connection.prototype.processEvent = function(msg) {
		var self = this;
		if (typeof msg === 'number') {
			self.tweetCount(self.tweetCount() + 1);
			view.totalEventCount(view.totalEventCount() + 1);
		} else {
			console.log(msg);
		}
	};

	Connection.prototype._update = function(json) {
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
			return left.type === right.type ? 0 : (left.type < right.type ? -1
					: 1)
		});
	};

	Connection.prototype.refresh = function() {
		var self = this;
		eb.send(self.address, {
			'command' : 'status'
		}, function(msg) {
			self._update(msg);
		});
	};

	Connection.prototype.lastN = function(n) {
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
			new Section('Controls', 'section-controls') ]);

	self.selectedSection = ko.observable(self.sections()[0]);

	self.selectSection = function(s) {
		self.selectedSection().selected(false);
		self.selectedSection(s);
		self.selectedSection().selected(true);
	}

	self.selectSection(self.sections()[0]);

	self.sniff = function(f) {
		self.selectConnection(null);
		self.selectSection(self.sections()[0]);
	}

	eb.onopen = function() {
		self.address = "hbbrowser" + "." + makeUUID();
		ko.applyBindings(self);
		eb.registerHandler(self.address + '.stati', function(msg) {
			console.log(msg);
			self.active.push(new Connection(msg));
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


		(function() {
			navigation.reset();
			navigation.push('#/Home', 'Hosebird Driver', self.clearSelected);
		})();

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
	json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g,
			'&gt;');
	return json
			.replace(
					/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g,
					function(match) {
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
