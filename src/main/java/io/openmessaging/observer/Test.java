package io.openmessaging.observer;

public class Test {

	
	public  static void  main(String[] args) {
		Long id1 = Thread.currentThread().getId() % 10;
		Long id = 12L % 10;
		System.err.println(id.intValue() + "");
	}
}
