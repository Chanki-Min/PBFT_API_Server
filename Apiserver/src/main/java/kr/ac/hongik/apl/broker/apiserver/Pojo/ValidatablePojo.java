package kr.ac.hongik.apl.broker.apiserver.Pojo;

public interface ValidatablePojo {
     /**
      * API에서 받아온 데이터가 요구 사항을 만족하는지 검사하고, 이 결과를 boolean으로 반환한다.
      *
      * @return 검증 결과
      */
     boolean validateMemberVar();
}
