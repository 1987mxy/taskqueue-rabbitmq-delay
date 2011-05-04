package cn.gamemate.taskqueue.rabbitmq.delay;

import java.util.*;

import cn.gamemate.taskqueue.*;
import cn.gamemate.taskqueue.rabbitmq.*;
import cn.gamemate.taskqueue.rabbitmq.imp.SingleChannelManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DelayService extends RabbitTaskQueueFactory{
	private int sendNumber,recvNumber;
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private static Map<Long, ArrayList<Task>> delayMap = new HashMap<Long, ArrayList<Task>>();
	private boolean run;
	
	private RabbitTaskQueueFactory service;
	private static final String rabbitTaskQueueServiceAddress = "dev.gamemate.cn";
	private static final long deferMaxTime = 2592000;
	
	private final TaskQueueConfiguration deferConfig = new TaskQueueConfiguration();
	private TaskQueue deferQueue;
	private static final String deferQueueName = "deferred_tasks";
	private ChannelManager channelManager;

	public DelayService(){
		run = true;
		this.deferConfig.setName(deferQueueName);   //set queue name
		this.deferConfig.setAmqpName(deferQueueName);   //the queue name will change QT_deferred_tasks if you don't set
		this.deferConfig.setAutoDelete(false);   //queue not auto delete
		this.deferConfig.setAsyncReceive(true);  //the queue can not receive tasks if asynchronism set false
		this.deferConfig.setAutoAck(true);  //you must call queue.ack(task) if the option is false, otherwise the received task is alive in service
		this.service = new RabbitTaskQueueFactory(rabbitTaskQueueServiceAddress);
		this.service.setQueues(Arrays.asList(this.deferConfig));
		this.service.prepareAndBind();
		deferQueue = TaskQueueFactory.getQueue(deferQueueName);
		channelManager = new SingleChannelManager(service.getConnectionManager());
	}
	
	public void receiveTask() {
		Task task;
		long sendTime;
		
		while (this.run) {
			logger.debug("========account task========");
			//output to log amount of receive tasks and send tasks 
			logger.debug("send package amount is {}", this.sendNumber);
			logger.debug("recv package amount is {}", this.recvNumber);
			
			task = deferQueue.receive(1000);
			this.sendTask();

			logger.debug("========receive task========");
			
			if (task == null || task.getDefer() > deferMaxTime || task.getDefer() < 0){
				continue;
			}
			
			sendTime = task.getDefer() * 1000 + System.currentTimeMillis();
			if(delayMap.keySet().contains(sendTime)){
				logger.debug("receive task {} append to sendQueue {}", task.getName(), sendTime);
				delayMap.get(sendTime).add(task);
			}else{
				logger.debug("receive task {} create and add sendQueue {}", task.getName(), sendTime);
				delayMap.put(sendTime, new ArrayList<Task>(Arrays.asList(task)));
			}
			//recode receive tasks amount
			this.recvNumber ++;
		}
	}

	private void sendTask() {
		Long sendTime,nowTime;
		String normalQueueName;
		Map.Entry<Long, ArrayList<Task>> item;
		
		logger.debug("========send task========");
		
		for (Iterator<Map.Entry<Long, ArrayList<Task>>> entrySet = delayMap.entrySet().iterator();entrySet.hasNext();) {
			item = entrySet.next();
			sendTime = item.getKey();
			nowTime = System.currentTimeMillis();
			
			if (sendTime <= nowTime) {
				for(Task task : item.getValue()){
					normalQueueName = task.getParam("amqp_target");
					try {
						channelManager.getChannel().basicPublish(this.service.getExchangeName(), normalQueueName, null, task.toByteArray());
						logger.debug("task {} send to queue {}", task.getName(), normalQueueName);
						//task sended and send amount add 1
						this.sendNumber ++;
						
					} catch (Exception e) {
						logger.debug(String.format("send task %s error : ", normalQueueName), e.getStackTrace());
					}
				}
				entrySet.remove();
			}
			else{
				for(Task task : item.getValue()){
					logger.debug("request send task {} after {}ms", task.getName(), sendTime - nowTime);
				}
			}
		}
	}

}

final public class Main{
	private Main(){}
	public static void main(String[] args) {
		DelayService dService = new DelayService();
		dService.receiveTask();
	}
}
