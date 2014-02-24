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

(function HosebirdDriverModel() {
	var self = this;
	var eb = new vertx.EventBus(window.location.protocol + '//'
			+ window.location.hostname + ':' + window.location.port
			+ '/eventbus');
	window.model = this;
	self.tweetCount = ko.observable(0);
	self.active = ko.observableArray([]);
	self.total = ko.computed(function () { return self.active().length });
	self.selected = ko.observable(null);
		
	self.tweetCount.subscribe(function (p) {
		$(".tweet-count-progress").attr("style", "width: " + Math.floor((p / 10) % 100) + "%");
	});
		
	// set interval
	var tid = setInterval(function () {
		if (!self.selected()) {
			return;
		}
		self.selected().refresh();
		self.selected().last(10);
	}, 3000);

	ko.applyBindings(self);

	eb.onopen = function() {
		eb
				.registerHandler(
						'hbdriver.events',
						function(msg) {
							if (msg.e === 1) {
								self.tweetCount(self.tweetCount() + 1);
							}
						});
		eb.registerHandler('hbbrowser.stati', function (msg) {
			console.log(msg);
			self.active.push(new Client(msg));
		});
		eb.publish('hbdriver.stati', {
			replyTo: 'hbbrowser.stati'
		});
	};


	eb.onclose = function() {
		eb = null;
	};
	
	self.selectClient = function (client) {
		console.log(client);
		self.selected(client);
	};

	function Client(json) {
		var self = this;
		self.name = json.address.replace(/.*:/, "");
		self.address = json.address;
		self.stats = ko.observableArray([]);
		self.statValues = {};
		self.config = ko.observable();
		self._update(json);
		self.lastN = ko.observableArray([]);
		self.maxLength = 20;
	}
	
	Client.prototype._update = function (json) {
		var self = this;
		self.config(json.config);
		for (var type in json.stats) {
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
	};
	
	Client.prototype.refresh = function () {
		var self = this;
		eb.send(self.address, 
				{'command': 'status'},
				function (msg) {
					self._update(msg);
				});
	};
	
	Client.prototype.last = function (n) {
		var self = this;
		eb.send(self.address, 
				{'command': 'last_n', 'n': n},
				function (msg) {
					for (var i = 0; i < msg.items.length; i++) {
						var m = msg.items[i];
						if (m.text) {
							m.url = "https://twitter.com/" + m.user.screen_name + "/status/" + m.id;
						}
						self.lastN.unshift(m);
					}
					while (self.lastN().length > self.maxLength) {
						self.lastN.pop();
					}
				});
	};
	
	
//
//	self.orderSubmitted = ko.observable(false);
//
//	self.submitOrder = function() {
//
//		if (!orderReady()) {
//			return;
//		}
//
//		var orderItems = ko.toJS(self.items);
//		var orderMsg = {
//			action : "save",
//			collection : "orders",
//			document : {
//				username : self.username(),
//				items : orderItems
//			}
//		}
//
//		eb.send('vertx.mongopersistor', orderMsg, function(reply) {
//			if (reply.status === 'ok') {
//				self.orderSubmitted(true);
//				// Timeout the order confirmation box after 2 seconds
//				// window.setTimeout(function() { self.orderSubmitted(false); },
//				// 2000);
//			} else {
//				console.error('Failed to accept order');
//			}
//		});
//	};
//
//	self.username = ko.observable('');
//	self.password = ko.observable('');
//	self.loggedIn = ko.observable(false);
//
//	self.login = function() {
//		if (self.username().trim() != '' && self.password().trim() != '') {
//			eb.login(self.username(), self.password(), function(reply) {
//				if (reply.status === 'ok') {
//					self.loggedIn(true);
//				} else {
//					alert('invalid login');
//				}
//			});
//		}
//	}
//
//	function Album(json) {
//		var self = this;
//		self._id = json._id;
//		self.genre = json.genre;
//		self.artist = json.artist;
//		self.title = json.title;
//		self.price = json.price;
//		self.formattedPrice = ko.computed(function() {
//			return '$' + self.price.toFixed(2);
//		});
//	}
//
//	function CartItem(album) {
//		var self = this;
//		self.album = album;
//		self.quantity = ko.observable(1);
//	}
//	
//	
})();