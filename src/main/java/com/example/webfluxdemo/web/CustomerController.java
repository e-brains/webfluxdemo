package com.example.webfluxdemo.web;

import com.example.webfluxdemo.domain.Customer;
import com.example.webfluxdemo.domain.CustomerRepository;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;

@RestController
public class CustomerController {

    private final CustomerRepository customerRepository;

    // 클라이언트와의 연결을 끊지 않고 지속하고 싶을때
    private final Sinks.Many<Customer> sink; // 모든 flux 요청들의 stream을 merge해서 sink를 맞춰 준다.



    public CustomerController(CustomerRepository customerRepository) {
        this.customerRepository = customerRepository;
        this.sink = Sinks.many().multicast().onBackpressureBuffer(); // multicast()는 신규로 추가된 데이터만 전송한다.
    }

    @GetMapping("/customer")
    public Flux<Customer> findAll(){
        return customerRepository.findAll().log(); // jpa문법과 동일

        // log 출력 결과
        /*
        onSubscribe(FluxUsingWhen.UsingWhenSubscriber) // 데이터베이스에 subscribe하여 구독정보를 받음
        request(unbounded)  // 데이터를 한번에 모두 달라는 의미 , 1이면 1건만 줌
        onNext(Customer(id=1, firstName=Jack, lastName=Bauer))  // 차례로 DB에서 데이터를 읽은다.
        onNext(Customer(id=2, firstName=Chloe, lastName=O'Brian))
        onNext(Customer(id=3, firstName=Kim, lastName=Bauer))
        2onNext(Customer(id=4, firstName=David, lastName=Palmer))
        onNext(Customer(id=5, firstName=Michelle, lastName=Dessler))
        onComplete()  // 모든 데이터를 읽게 되면 클라이언트로 전송(응답)

         */

    }

    @GetMapping("/flux")
    public Flux<Integer> flux(){
        return Flux.just(1,2,3,4,5).delayElements(Duration.ofSeconds(1)).log(); //1초간격으로 찍고 onComplete되면 전송
    }


    // HTTP에서 사용하는 MediaType을 스트림으로 설정해 주면 클라이언트에 onNext되면서 계속 전송한다.
    @GetMapping(value = "/fluxstream", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
    public Flux<Integer> fluxstream(){
        return Flux.just(1,2,3,4,5).delayElements(Duration.ofSeconds(1)).log(); //1초간격으로 찍고 onNext시 전송한다.
    }


    // id로 조회 시 1건이기 때문에 Mono타입을 사용, 여러건이면 Flux 타입을 사용한다.
    @GetMapping("customer/{id}")
    public Mono<Customer> findById(@PathVariable Long id){
        return customerRepository.findById(id).log(); // onNext 한번에 complete한다.
    }


    // 여러건을 순차적으로 화면에 보여주고 싶을 때
    // 단 MediaType.APPLICATION_STREAM_JSON_VALUE은 스트림이 끝나면 연결을 닫는다.
    @GetMapping(value = "/customer/stream", produces = MediaType.APPLICATION_STREAM_JSON_VALUE)
    public Flux<Customer> findAllStream(){
        return customerRepository.findAll().delayElements(Duration.ofSeconds(1)).log();

        /* 결과 출력
        {"id":1,"firstName":"Jack","lastName":"Bauer"}
        {"id":2,"firstName":"Chloe","lastName":"O'Brian"}
        {"id":3,"firstName":"Kim","lastName":"Bauer"}
        {"id":4,"firstName":"David","lastName":"Palmer"}
        {"id":5,"firstName":"Michelle","lastName":"Dessler"}
         */

    }

    // sse 프로토콜을 적용해서도 화면에 순차적으로 보여 줄 수 있다.
    // 특징은 화면에 data라는 단어가 추가해서 보여진다.
    @GetMapping(value = "/customer/sse", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Customer> findAllSSE(){
        return customerRepository.findAll().delayElements(Duration.ofSeconds(1)).log();

        /* 결과 출력
        data:{"id":1,"firstName":"Jack","lastName":"Bauer"}
        data:{"id":2,"firstName":"Chloe","lastName":"O'Brian"}
        data:{"id":3,"firstName":"Kim","lastName":"Bauer"}
        data:{"id":4,"firstName":"David","lastName":"Palmer"}
        data:{"id":5,"firstName":"Michelle","lastName":"Dessler"}
         */
    }

    // 모든 Flux 요청들을 하나로 묶어서 관리한다. ( 새로운 push된 데이터가 있어야 화면에 나옴 )
    // 이 테스트를 위해서는 PostMapping을 이용하여 데이터를 push해야 한다.
    @GetMapping("/customer/sink") // ServerSentEvent 타입을 리턴하면 produces = MediaType.TEXT_EVENT_STREAM_VALUE)은 생략 가능
    public Flux<ServerSentEvent<Customer>> findAllSink() {
        return sink.asFlux().map(customer -> ServerSentEvent.builder(customer).build())  //asFlux()로 합쳐진 데이터를 리턴
                // push하는 순간 다른 요청자 화면에 push한 데이터가 보여진다.
                // 단점 : 사용자가 브라우저에서 요청을 강제로 끊으면 서버에서는 끊었는지 알 수 없기 때문에 다시 요청해서 연결을 이어갈 수 없다.
                // 그래서 이에 대한 처리를 해 줘야 한다.
                .doOnCancel(()->{ // 취소 될때
                    sink.asFlux().blockLast();  // blockLast()를 해주면 마지막 데이터로 인식 onComplete가 자동 호출되고
                                                // 클라이언트에서 다시 요청하면 다시 sink가 맞춰져서 연결을 이어 갈 수 있다.
                                                // 이 모든것이 단일 쓰레드로 동작함
                });

    }



    // 클라이언트 요청 테스트
    @PostMapping("/customer")  // postman 같은 툴을 이용하여 호출
    public Mono<Customer> save(){
        return customerRepository.save(new Customer("gildong", "Hong"))  // 데이터 생성
                .doOnNext(customer -> {sink.tryEmitNext(customer);  // sink로 데이터 push -> sink.asFlux()가 알아차림
                });
    }



}
