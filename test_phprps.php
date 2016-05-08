<?php

require_once "phprps.php";

class PHPRpsTestCase  extends PHPUnit_Framework_TestCase {
    protected $phprps;

    public function setUp() {
        $namespace = "phprpstestcase";

        $this->phprps = new \phprps\PHPRps($namespace);

        foreach ($this->phprps->getRedis()->keys(sprintf("%s.*", $namespace)) as $key) {
            $this->phprps->getRedis()->delete($key);
        }
    }

    public function testConnectionWorking() {
        $this->phprps->getRedis()->ping();
    }

    public function testPublishWithoutReader() {
        $this->phprps->publish("test", "test message content");
        $sub = $this->phprps->subscribe("test", "consumer");

        $this->assertNull($sub->consume(false));
    }

    public function testPublishWithReader() {
        $sub = $this->phprps->subscribe("test", "consumer");
        $this->phprps->publish("test", "test message content");

        $this->assertEquals("test message content", $sub->consume(false));
    }
}
