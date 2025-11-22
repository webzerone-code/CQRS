<?php

use Illuminate\Foundation\Inspiring;
use Illuminate\Support\Facades\Artisan;
use RdKafka\Producer;

Artisan::command('inspire', function () {
    $this->comment(Inspiring::quote());
})->purpose('Display an inspiring quote');

Artisan::command('kafka:push {message}', function ($message) {
    $conf = new \RdKafka\Conf();
    $conf->set('metadata.broker.list', 'kafka:29092');

    $producer = new Producer($conf);
    $topic = $producer->newTopic('test-topic');

    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
    $producer->flush(10000);

    $this->info("Message sent to Kafka: $message");
});

Artisan::command('kafka:consume', function () {
    $conf = new \RdKafka\Conf();
    $conf->set('group.id', 'laravel-consumer');
    $conf->set('metadata.broker.list', 'kafka:29092');

    $consumer = new \RdKafka\KafkaConsumer($conf);
    $consumer->subscribe(['test-topic']);

    $this->info("Listening for messages...");

    while (true) {
        $message = $consumer->consume(120*1000);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $this->info("Received: " . $message->payload);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $this->warn("Timed out");
                break;
            default:
                $this->error($message->errstr());
                break;
        }
    }
});
