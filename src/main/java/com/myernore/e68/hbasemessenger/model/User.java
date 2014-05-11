package com.myernore.e68.hbasemessenger.model;

import java.util.List;

import org.joda.time.DateTime;

public class User {

	/********* Column Family: info ********/
	public String username;
	public String name;

	/********* Column Family: messages ********/
	public List<Message> messages;

	@Override
	public String toString() {
		return String.format("User: %s, %s with %d messages", username, name, messages != null ? messages.size() : 0);
	}
	
	public class Message {
		
		public String body;
		public DateTime date;
		public User fromUsername;
		public User toUsername;
		
	}
}
