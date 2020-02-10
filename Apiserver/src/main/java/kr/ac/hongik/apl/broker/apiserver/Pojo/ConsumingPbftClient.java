package kr.ac.hongik.apl.broker.apiserver.Pojo;

public interface ConsumingPbftClient {
	/**
	 * 컨슈머가 어떤 타입을 key-value로 가져올지는 도메인 마다 다르므로 객체 생성시 정의한다
	 */

	/**
	 * 컨슈머가 돌아가지 않고 있다면, 컨슈머 객체를 생성하고, loop를 돌면서 poll() 하고 일정 조건 만족시 execute()를 호출합니다
	 * @return
	 */
	public abstract Exception startConsumer();

	/**
	 * 조건 만족시 실제 실행 로직은 여기에 작성한다
	 */
	/*
	TODO : execute 로직을 완전히 수정하여야 한다.
		현재는 consumer와 execute()가 한 스레드(작업)에서 돌아간다. 즉 consumer.wakeup()을 콜하게 되면
		execute()작업이 수행중에 종료될 가능성이 있다. 즉 삽입중인 데이터가 유실될 수 있다.
		execute()를 호출하는 스레드는 별도의 풀에서 관리되어야 하며, 해당 스레드는 destroy()전에 모든 작업이 끝날 때까지 대기시켜야 한다.
	 */
	public abstract void execute(Object obj);

	/**
	 * AtomicBoolean과 Consumer.wakeup()을 통하여 thread-safe 하게 컨슈머를 종료합니다
	 */
	public abstract void shutdownConsumer();

	/**
	 * 객체를 멈추기 위해 실행할 로직을 작성합니다
	 */
	public abstract void destroy() throws Exception;
}
