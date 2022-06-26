package java_kafka;

import java.util.concurrent.ExecutionException;

public class mainn {

	
	
	public static void main (String [] args) {
		
		String server = "127.0.0.1:9092";
		String topic = "user_registered";
		String groupId = "some_application";
		
		new Thread(new Runnable() {
		    public void run() {
		    	Producer producer = new Producer(server);
		    	try {
		    		producer.put(topic, "user1", "John");
		    	} catch (ExecutionException e1) {
		    		// TODO Auto-generated catch block
		    		e1.printStackTrace();
		    	} catch (InterruptedException e1) {
		    		// TODO Auto-generated catch block
		    		e1.printStackTrace();
		    	}
		    	try {
		    		producer.put(topic, "user2", "Peter");
		    	} catch (ExecutionException e) {
		    		// TODO Auto-generated catch block
		    		e.printStackTrace();
		    	} catch (InterruptedException e) {
		    		// TODO Auto-generated catch block
		    		e.printStackTrace();
		    	}
		    	producer.close();
		    }
		}).start();

		new Thread(new Runnable() {
		    public void run() {
		
				new Consumer(server, groupId, topic).run(); 
		    }
		}).start();
	}

}
