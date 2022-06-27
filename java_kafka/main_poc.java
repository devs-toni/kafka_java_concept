package java_kafka;

import java.util.concurrent.ExecutionException;

public class main_poc {

	
	
	public static void main (String [] args) {
		
		String server = "localhost:9092";
		String topic = "user_registered";
		String groupId = "swarm";
		
		new Thread(new Runnable() {
		    public void run() {
		    	producer producer = new producer(server);
		    	try {
		    		producer.put(topic, "user1", "Antonio");
		    	} catch (ExecutionException e1) {
		    		// TODO Auto-generated catch block
		    		e1.printStackTrace();
		    	} catch (InterruptedException e1) {
		    		// TODO Auto-generated catch block
		    		e1.printStackTrace();
		    	}
		    	
		    	
		    	try {
		    		producer.put(topic, "user2", "Pepe");
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
		
				new consumer(server, topic).run(); 
		    }
		}).start();
	}

}
