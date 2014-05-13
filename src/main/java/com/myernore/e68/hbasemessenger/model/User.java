package com.myernore.e68.hbasemessenger.model;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class User {

	private DateTimeFormatter dtf = DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss a");
	
	/********* Column Family: info ********/
	public String username;
	public String name;

	/********* Column Family: messages ********/
	public List<User.Message> messages;

	public User(String username) {
		this.username = username;
		messages = new ArrayList<User.Message>();
	}

	public List<Message> getMessages() {
		return messages;
	}

	public void addMessage(Message m) {
		messages.add(m);
	}

	@Override
	public String toString() {
		return String.format("User: %s, %s with %d messages", username, name,
				messages != null ? messages.size() : 0);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null) {
			return false;
		}

		User user = (User) obj;
		return username.equals(user.username);
	}

	public class Message {

		public String body;
		public DateTime date;
		public User otherUser;
		public boolean isFrom;

		public Message(User user, boolean isFrom, DateTime date, String body) {
			super();
			this.body = body;
			this.date = date;
			this.otherUser = user;
			this.isFrom = isFrom;
		}

		public Message(User user, boolean isFrom, String body) {
			this( user, isFrom, new DateTime(), body);
		}
		
		public User getHostUser() {
			return User.this;
		}
		
		public User getOtherUser() {
			return otherUser;
		}
		
		@Override
		public String toString() {
			String fromSpacing = "           ";
			String fromOrTo = isFrom ? fromSpacing + "From " : "To ";
			return "-----------------------\n" 
			+ fromOrTo + getOtherUser().username 
			+ " at " + dtf.print(date) + "\n" + (isFrom ? fromSpacing : "") + body;
		}

	}
}
