package com.myernore.e68.HBaseMessenger.model;

public class User {

	public String user;
	public String name;

	@Override
	public String toString() {
		return String.format("<User: %s, %s>", user, name);
	}
}
