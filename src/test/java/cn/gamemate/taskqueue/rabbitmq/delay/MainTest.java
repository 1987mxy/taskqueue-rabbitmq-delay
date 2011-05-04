package cn.gamemate.taskqueue.rabbitmq.delay;

import org.junit.*;

import cn.gamemate.taskqueue.*;
import cn.gamemate.taskqueue.rabbitmq.*;

import java.util.*;

public class MainTest {
	private RabbitTaskQueueFactory service;
	private final String rabbitTaskQueueServiceAddress = "dev.gamemate.cn";
	private String normalQueueName1, normalQueueName2;
	private String deferQueueName;
	private Random randomGenerate = new Random(System.currentTimeMillis());

	@Before
	public void setUp() {
		this.service = new RabbitTaskQueueFactory(
				this.rabbitTaskQueueServiceAddress);

		this.deferQueueName = "dreferred_tasks";
		this.normalQueueName1 = String.valueOf(System.currentTimeMillis());
		this.normalQueueName2 = String.valueOf(System.currentTimeMillis() + 1);
		TaskQueueConfiguration deferConfig = new TaskQueueConfiguration();
		deferConfig.setName(this.deferQueueName);
		deferConfig.setAsyncReceive(true);
		deferConfig.setAutoAck(false);
		// deferConfig.setAutoDelete(false);
		TaskQueueConfiguration normalConfig1 = new TaskQueueConfiguration();
		normalConfig1.setName(this.normalQueueName1);
		normalConfig1.setAsyncReceive(true);
		normalConfig1.setAutoAck(false);
		// normalConfig1.setAutoDelete(true);
		TaskQueueConfiguration normalConfig2 = new TaskQueueConfiguration();
		normalConfig2.setName(this.normalQueueName2);
		normalConfig2.setAsyncReceive(true);
		normalConfig2.setAutoAck(false);
		// normalConfig2.setAutoDelete(true);

		this.service.setQueues(Arrays.asList(deferConfig, normalConfig1,
				normalConfig2));// delete
		// deferQueue

		this.service.prepare();

		this.service.deleteAllRabbitmqQueues();

		this.service = new RabbitTaskQueueFactory(
				this.rabbitTaskQueueServiceAddress);

		this.service.setQueues(Arrays.asList(deferConfig, normalConfig1,
				normalConfig2));
		// deferQueue

		this.service.prepareAndBind();

	}

	@After
	public void tearDown() {
		this.service.deleteAllRabbitmqQueues();
		this.service.unbind();
	}

	@Test
	public void testDelay() {
		try {
			RabbitTaskQueue normalQueue = this.service
					.getQueue(this.normalQueueName1);
			Task sendTask = this.generateTask();
			sendTask.param("amqp_target", "TQ_" + this.normalQueueName1);

			long sendTime = System.currentTimeMillis();
			System.out.println(String.valueOf(sendTask.getDefer()));
			System.out.println(sendTask.getName());
			normalQueue.add(sendTask);

			long receiveTime;
			Task receiveTask;
			while (true) {
				receiveTask = normalQueue.receive(1000);
				receiveTime = System.currentTimeMillis();
				if (receiveTask == null) {
					continue;
				}
				normalQueue.ack(receiveTask);

				Assert.assertEquals(receiveTask.getParam("amqp_target"), "TQ_"
						+ normalQueue.getName());

				int deferTime = receiveTask.getDefer();

				Assert.assertTrue((sendTime + deferTime * 1000) <= receiveTime);
				this.taskEquals(sendTask, receiveTask);
				break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testMultitask() {

		RabbitTaskQueue normalQueue = this.service
				.getQueue(this.normalQueueName1);

		HashMap<String, Task> sendTasks = this.generateTask(10);

		for (String taskName : sendTasks.keySet()) {
			Task sendTask = sendTasks.get(taskName);
			sendTask.param("amqp_target", "TQ_" + this.normalQueueName1);
			sendTask.param("sendTime",
					String.valueOf(System.currentTimeMillis()));
			normalQueue.add(sendTask);
		}

		Task receiveTask, sendTask;
		long receiveTime, sendTime;
		int deferTime;
		while (true) {
			receiveTask = normalQueue.receive(1000);
			receiveTime = System.currentTimeMillis();

			// test
			for (String taskName : sendTasks.keySet()) {
				if (Long.valueOf(sendTasks.get(taskName).getParam("sendTime"))
						+ sendTasks.get(taskName).getDefer() * 1000 + 1000 <= receiveTime) {
					System.out
							.println("don't recv package name is " + taskName);
				}
			}

			if (receiveTask == null) {
				continue;
			}
			normalQueue.ack(receiveTask);

			Assert.assertEquals(receiveTask.getParam("amqp_target"), "TQ_"
					+ normalQueue.getName());

			String taskName = receiveTask.getName();

			if (sendTasks.containsKey(taskName)) {
				sendTask = sendTasks.get(taskName);
				deferTime = receiveTask.getDefer();
				sendTime = Long.valueOf(receiveTask.getParam("sendTime"));

				Assert.assertTrue((sendTime + deferTime * 1000) <= receiveTime);
				this.taskEquals(sendTask, receiveTask);
				sendTasks.remove(taskName);
				System.out.println("receive task and effect "
						+ receiveTask.getName());
				System.out.println("spilth task is "
						+ sendTasks.entrySet().toArray().length);
				if (sendTasks.isEmpty()) {
					break;
				}
			}
		}
	}

	@Test
	public void testBlendtask() {

		RabbitTaskQueue normalQueue1 = this.service
				.getQueue(this.normalQueueName1);
		RabbitTaskQueue normalQueue2 = this.service
				.getQueue(this.normalQueueName2);

		HashMap<String, Task> sendTasks = this.generateTask(10);

		for (String taskName : sendTasks.keySet()) {
			Task sendTask = sendTasks.get(taskName);
			if (randomGenerate.nextBoolean()) {
				sendTask.param("amqp_target", "TQ_" + this.normalQueueName1);
				sendTask.param("sendTime",
						String.valueOf(System.currentTimeMillis()));
				normalQueue1.add(sendTask);
			} else {
				sendTask.param("amqp_target", "TQ_" + this.normalQueueName2);
				sendTask.param("sendTime",
						String.valueOf(System.currentTimeMillis()));
				normalQueue2.add(sendTask);
			}
		}

		Task receiveTask, sendTask;
		long receiveTime, sendTime;
		TaskQueue normalQueue;
		int deferTime;
		boolean queueSelect = true;
		while (true) {
			if (queueSelect) {
				normalQueue = normalQueue1;
			} else {
				normalQueue = normalQueue2;
			}
			queueSelect = !queueSelect;
			receiveTask = normalQueue.receive(1000);
			receiveTime = System.currentTimeMillis();

			// test
			for (String taskName : sendTasks.keySet()) {
				if (Long.valueOf(sendTasks.get(taskName).getParam("sendTime"))
						+ sendTasks.get(taskName).getDefer() * 1000 + 1000 <= receiveTime) {
					System.out
							.println("don't recv package name is " + taskName);
				}
			}

			if (receiveTask == null) {
				continue;
			}
			normalQueue.ack(receiveTask);

			Assert.assertEquals(receiveTask.getParam("amqp_target"), "TQ_"
					+ normalQueue.getName());

			String taskName = receiveTask.getName();

			if (sendTasks.containsKey(taskName)) {
				sendTask = sendTasks.get(taskName);
				deferTime = receiveTask.getDefer();
				sendTime = Long.valueOf(receiveTask.getParam("sendTime"));

				Assert.assertTrue((sendTime + deferTime * 1000) <= receiveTime);
				this.taskEquals(sendTask, receiveTask);
				sendTasks.remove(taskName);
				System.out.println("receive task and effect "
						+ receiveTask.getName());
				System.out.println("spilth task is "
						+ sendTasks.entrySet().toArray().length);
				if (sendTasks.isEmpty()) {
					break;
				}
			}
		}
	}

	private void taskEquals(Task taskA, Task taskB) {
		byte[] taskABytes = taskA.toByteArray();
		byte[] taskBBytes = taskB.toByteArray();
		for (int index = 0; index < taskABytes.length; index++) {
			Assert.assertEquals(taskABytes[index], taskBBytes[index]);
		}
	}

	private HashMap<String, Task> generateTask(int len) {
		HashMap<String, Task> tasks = new HashMap<String, Task>();
		Task sendTask;
		String taskName = null;

		for (int index = 0; index < len; index++) {
			sendTask = new Task();
			while (index == 0 || tasks.keySet().contains(taskName)) {
				taskName = String.valueOf(randomGenerate.nextInt());
				if (index == 0) {
					break;
				}
			}
			sendTask.name(taskName);
			sendTask.defer(randomGenerate.nextInt(20) + 1);
			sendTask.param(String.valueOf(randomGenerate.nextInt()),
					String.valueOf(randomGenerate.nextInt()));
			tasks.put(taskName, sendTask);
		}

		return tasks;
	}

	private Task generateTask() {
		Task sendTask = new Task();

		sendTask.name(String.valueOf(randomGenerate.nextInt()));
		sendTask.defer(randomGenerate.nextInt(20) + 1);
		sendTask.param(String.valueOf(randomGenerate.nextInt()),
				String.valueOf(randomGenerate.nextInt()));
		return sendTask;
	}

}
