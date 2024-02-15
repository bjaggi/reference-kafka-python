confluent_cluster_config = {
    'bootstrap.servers': 'pkc-ep9mm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'CUELITMMWZFYAJM3',
    'sasl.password': 'kZN1W+mFZdNp4qytzgU3qWxBbmJ1YLMZviONTNMTK0Qvt8vIhkp3X7Krzqgk3Gts',
    'client.id': 'jaggi_laptop_client',
    'delivery.timeout.ms' : '10000',
    'request.timeout.ms' : '10000',
    'batch.size' : '55000',
    'linger.ms' : '50',
    'compression.type': 'zstd',
    'acks': 'all'

   # 'idempotent.producer' : 'true'

}

confluent_sr_config = {
    'url': 'https://psrc-j55zm.us-central1.gcp.confluent.cloud',
    'basic.auth.user.info': 'W4ZYQ2242AWXRNFJ:INX4avuDPSlb9QstqdycWQqsF54DR/H8JQakYytCKgRORSgYCyrG/HhJ5CIzCgDY'
}

client_config = {
    'topic_name' : 'test',
    'consumer_group' : 'test_cg'
}


