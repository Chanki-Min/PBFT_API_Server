package kr.ac.hongik.apl.broker.apiserver.Pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@AllArgsConstructor
@Getter @Setter
public class BlockId {
	/**
	 * 블록체인 이름
	 */
	private String chainName;
	/**
	 * 블록 넘버
	 */
	private int blockNumber;


	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof BlockId))
			return false;
		else
			return this.chainName.equals(((BlockId) obj).chainName) && (this.blockNumber == ((BlockId) obj).blockNumber);
	}

	/**
	 * 항상 Java에서 equals를 오버라이딩 할때는 반드시 hashcode()도 오버라이딩 하는 습관을 가져야 한다! 이 hash코드는 논리적으로
	 * 같다고 받아들여지는 객체 사이에서 같은 결과값이 나와야 한다.
	 *
	 * @return instance의 hash
	 * ref) https://docs.oracle.com/javase/8/docs/api/java/util/stream/Stream.html#distinct--
	 */
	@Override
	public int hashCode() {
		return chainName.hashCode() + blockNumber;
	}
}
