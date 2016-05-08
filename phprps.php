<?php namespace phprps;

abstract class NamespacedRedis {
    protected $namespace;
    protected $redis;

    protected function __construct($namespace, \Redis $redis) {
        $this->namespace = $namespace;
        $this->redis = $redis;
    }

    protected function _ns() {
        return \sprintf("%s.%s", $this->namespace, \implode(".", \func_get_args()));
    }

    protected function _ns_subscriptions($queue) {
        return $this->_ns($queue, "consumers");
    }

    protected function _ns_nextid() {
        return $this->_ns("nextid");
    }

    protected function _ns_message($queue, $message_id) {
        return $this->_ns($queue, "messages", $message_id);
    }

    protected function _ns_queue($queue, $consumer_id) {
        return $this->_ns($queue, $consumer_id, "messages");
    }

    public function getRedis() {
        return $this->redis;
    }
}

class PHPRps extends NamespacedRedis {
    public function __construct($namespace, $redis_host="localhost", $redis_port=6379) {
        $redis = new \Redis();
        $redis->connect($redis_host, $redis_port);

        parent::__construct($namespace, $redis);
    }

    public function subscribe($queue, $consumer_id) {
        $this->redis->sAdd($this->_ns_subscriptions($queue), $consumer_id);

        return new Subscription($this, $queue, $consumer_id);
    }

    public function publish($queue, $message, $ttl=3600) {
        $message_id = $this->redis->incr($this->_ns_nextid());

        $this->redis->setex($this->_ns_message($queue, $message_id), $ttl, $message);

        $consumers = $this->redis->smembers($this->_ns_subscriptions($queue));

        foreach ($consumers as $consumer) {
            $this->redis->rpush($this->_ns_queue($queue, $consumer), $message_id);
        }
    }
}


class Subscription extends NamespacedRedis {
    protected $queue;
    protected $consumer_id;

    public function __construct($rps, $queue, $consumer_id) {
        parent::__construct($rps->namespace, $rps->redis);

        $this->queue = $queue;
        $this->consumer_id = $consumer_id;
    }

    public function consume($block=true, $timeout=0) {
        while (true) {
            if ($block) {
                $message_id = $this->redis->blpop($this->_ns_queue($this->queue, $this->consumer_id), $timeout);
                $message_id = $message_id[1];
            } else {
                $message_id = $this->redis->lpop($this->_ns_queue($this->queue, $this->consumer_id));

                if (\is_null($message_id) || $message_id === false) {
                    return NULL;
                }
            }

            $message = $this->redis->get($this->_ns_message($this->queue, $message_id));

            if (!\is_null($message)) {
                return $message;
            }
        }
    }

    public function unsubscribe() {
        $this->redis->srem($this->_ns_subscriptions($this->queue), $this->consumer_id);
        $this->redis->delete($this->_ns_queue($this->queue, $this->consumer_id));
    }
}

