docker build -f core\Dockerfile.base -t core_base .
 
docker build -f technologies\zeromq_p2p\Dockerfile.base -t zeromq_p2p_base .
docker build -f technologies\zeromq_p2p\Dockerfile.publisher -t zeromq_p2p_publisher .
docker build -f technologies\zeromq_p2p\Dockerfile.consumer -t zeromq_p2p_consumer .
 
docker build -f technologies\kafka_p2p\Dockerfile.base -t kafka_p2p_base .
docker build -f technologies\kafka_p2p\Dockerfile.publisher -t kafka_p2p_publisher .
docker build -f technologies\kafka_p2p\Dockerfile.consumer -t kafka_p2p_consumer .
 
docker build -f technologies\arrowflight_p2p\Dockerfile.base -t arrowflight_p2p_base .
docker build -f technologies\arrowflight_p2p\Dockerfile.publisher -t arrowflight_p2p_publisher .
docker build -f technologies\arrowflight_p2p\Dockerfile.consumer -t arrowflight_p2p_consumer .
 
 
docker build -f technologies\arrowflight_bin_p2p\Dockerfile.base -t arrowflight_bin_p2p_base .
docker build -f technologies\arrowflight_bin_p2p\Dockerfile.publisher -t arrowflight_bin_p2p_publisher .
docker build -f technologies\arrowflight_bin_p2p\Dockerfile.consumer -t arrowflight_bin_p2p_consumer .
 
docker build -f technologies\nats_p2p\Dockerfile.base -t nats_p2p_base .
docker build -f technologies\nats_p2p\Dockerfile.publisher -t nats_p2p_publisher .
docker build -f technologies\nats_p2p\Dockerfile.consumer -t nats_p2p_consumer .
 
docker build -f technologies\rabbitmq_p2p\Dockerfile.base -t rabbitmq_p2p_base .
docker build -f technologies\rabbitmq_p2p\Dockerfile.publisher -t rabbitmq_p2p_publisher .
docker build -f technologies\rabbitmq_p2p\Dockerfile.consumer -t rabbitmq_p2p_consumer .
 
docker build -f technologies/nats_jetstream_p2p/Dockerfile.base -t nats_jetstream_p2p_base .
docker build -f technologies/nats_jetstream_p2p/Dockerfile.publisher -t nats_jetstream_p2p_publisher .
docker build -f technologies/nats_jetstream_p2p/Dockerfile.consumer -t nats_jetstream_p2p_consumer .