package com.myernore.e68.hbasemessenger.model;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;

public class User {

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
		return String.format("User: %s, %s with %d messages", username, name, messages != null ? messages.size() : 0);
	}
	
	@Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null ) {
            return false;
        }

        User user = (User) obj;
        return username.equals(user.username);
    }

	public class Message {
		
		public String body;
		public DateTime date;
		public User fromUser;
		public User toUser;
		
		public Message(User fromUser,
				User toUser, DateTime date, String body ) {
			super();
			this.body = body;
			this.date = date;
			this.fromUser = fromUser;
			this.toUser = toUser;
		}
		
		public Message(User fromUser,
				User toUser, String body) {
			this( fromUser, toUser, new DateTime(), body);
		}
		
		public Message(String body, String fromUsername, String toUsername) {
			this( new User(fromUsername), new User(toUsername), body);
		}
		
	}
}
